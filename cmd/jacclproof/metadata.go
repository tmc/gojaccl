package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type metadataOptions struct {
	Device           string
	Remote           string
	RemoteTmp        string
	Root             string
	Artifact         string
	Stamp            string
	MaxGIDs          int
	ExpectedGIDIndex int
	Timeout          time.Duration
	SSHTimeout       time.Duration
}

type metadataProfile struct {
	device           string
	defaultMaxGIDs   int
	defaultTimeout   time.Duration
	confirmation     string
	confirmationText string
	requiresExpected bool
	rdmaStatusMode   string
}

func runRDMAMetadataPacket(args []string, stdout, stderr io.Writer) error {
	opts, profile, err := parseMetadataOptions(args, stderr)
	if err != nil {
		return err
	}
	if os.Getenv(profile.confirmation) != "one-shot-metadata" {
		fmt.Fprint(stderr, profile.confirmationText)
		return exitError{code: 2}
	}
	if err := validateMetadataOptions(opts, profile); err != nil {
		return exitError{code: 2, err: err}
	}
	r := metadataRun{opts: opts, profile: profile, stdout: stdout}
	return r.run()
}

func parseMetadataOptions(args []string, stderr io.Writer) (metadataOptions, metadataProfile, error) {
	fs := flag.NewFlagSet("rdma-metadata", flag.ContinueOnError)
	fs.SetOutput(stderr)
	device := fs.String("device", getenv("DEVICE", "rdma_en1"), "RDMA device")
	remote := fs.String("remote", os.Getenv("REMOTE"), "peer SSH target")
	remoteTmp := fs.String("remote-tmp", os.Getenv("REMOTE_TMP"), "peer writable artifact directory")
	root := fs.String("root", os.Getenv("ROOT"), "repository root")
	artifact := fs.String("art", os.Getenv("ART"), "local artifact directory")
	stamp := fs.String("stamp", os.Getenv("STAMP"), "UTC artifact stamp")
	maxGIDs := fs.Int("max-gids", getenvInt("MAX_GIDS", 0), "maximum GID table entries to query")
	expected := fs.Int("expected-selected-gid-index", getenvInt("EXPECTED_SELECTED_GID_INDEX", -1), "required selected GID index, or -1 to skip")
	timeout := fs.Duration("timeout", getenvDurationSeconds("TIMEOUT_SECONDS", 0), "per-command timeout")
	sshTimeout := fs.Duration("ssh-connect-timeout", getenvDurationSeconds("SSH_CONNECT_TIMEOUT", 10*time.Second), "SSH connect timeout")
	if err := fs.Parse(args); err != nil {
		return metadataOptions{}, metadataProfile{}, exitError{code: 2, err: err}
	}
	if fs.NArg() != 0 {
		return metadataOptions{}, metadataProfile{}, exitError{code: 2, err: fmt.Errorf("unexpected rdma-metadata arguments")}
	}
	profile, err := metadataProfileForDevice(*device)
	if err != nil {
		return metadataOptions{}, metadataProfile{}, exitError{code: 2, err: err}
	}
	if *maxGIDs == 0 {
		*maxGIDs = profile.defaultMaxGIDs
	}
	if *timeout == 0 {
		*timeout = profile.defaultTimeout
	}
	if *stamp == "" {
		*stamp = time.Now().UTC().Format("20060102T150405Z")
	}
	if *root == "" {
		wd, err := os.Getwd()
		if err != nil {
			return metadataOptions{}, metadataProfile{}, fmt.Errorf("getwd: %w", err)
		}
		*root = wd
	}
	if *artifact == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return metadataOptions{}, metadataProfile{}, fmt.Errorf("home directory: %w", err)
		}
		*artifact = filepath.Join(home, "tmp", fmt.Sprintf("gojaccl-%s-metadata-%s", artifactDeviceName(*device), *stamp))
	}
	opts := metadataOptions{
		Device:           *device,
		Remote:           *remote,
		RemoteTmp:        *remoteTmp,
		Root:             *root,
		Artifact:         *artifact,
		Stamp:            *stamp,
		MaxGIDs:          *maxGIDs,
		ExpectedGIDIndex: *expected,
		Timeout:          *timeout,
		SSHTimeout:       *sshTimeout,
	}
	return opts, profile, nil
}

func metadataProfileForDevice(device string) (metadataProfile, error) {
	switch device {
	case "rdma_en1":
		return metadataProfile{
			device:           device,
			defaultMaxGIDs:   1024,
			defaultTimeout:   40 * time.Second,
			confirmation:     "CONFIRM_RDMA_EN1_METADATA_ONE_SHOT",
			confirmationText: rdmaEn1MetadataRefusal,
			requiresExpected: true,
			rdmaStatusMode:   "enabled",
		}, nil
	case "rdma_en3":
		return metadataProfile{
			device:           device,
			defaultMaxGIDs:   64,
			defaultTimeout:   20 * time.Second,
			confirmation:     "CONFIRM_RDMA_EN3_METADATA_ONE_SHOT",
			confirmationText: rdmaEn3MetadataRefusal,
			rdmaStatusMode:   "device-active",
		}, nil
	default:
		return metadataProfile{}, fmt.Errorf("unsupported rdma metadata device %q", device)
	}
}

func validateMetadataOptions(opts metadataOptions, profile metadataProfile) error {
	if opts.Remote == "" {
		return fmt.Errorf("remote is required")
	}
	if opts.RemoteTmp == "" {
		return fmt.Errorf("remote-tmp is required")
	}
	if opts.MaxGIDs <= 0 {
		return fmt.Errorf("max-gids %d must be positive", opts.MaxGIDs)
	}
	if opts.Timeout <= 0 {
		return fmt.Errorf("timeout %s must be positive", opts.Timeout)
	}
	if opts.SSHTimeout <= 0 {
		return fmt.Errorf("ssh-connect-timeout %s must be positive", opts.SSHTimeout)
	}
	if profile.requiresExpected && opts.ExpectedGIDIndex < 0 {
		return fmt.Errorf("expected-selected-gid-index is required for %s", opts.Device)
	}
	return nil
}

type metadataRun struct {
	opts    metadataOptions
	profile metadataProfile
	stdout  io.Writer

	head      string
	short     string
	remoteArt string
	ctlBin    string
	proofBin  string
}

func (r *metadataRun) run() error {
	if err := os.MkdirAll(filepath.Join(r.opts.Artifact, "local"), 0777); err != nil {
		return fmt.Errorf("create local artifact dir: %w", err)
	}
	for _, dir := range []string{"remote", "proof"} {
		if err := os.MkdirAll(filepath.Join(r.opts.Artifact, dir), 0777); err != nil {
			return fmt.Errorf("create %s artifact dir: %w", dir, err)
		}
	}
	var err error
	r.head, err = commandOutput(r.opts.Root, "git", "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	r.short, err = commandOutput(r.opts.Root, "git", "rev-parse", "--short=12", "HEAD")
	if err != nil {
		return err
	}
	r.remoteArt = filepath.Join(r.opts.RemoteTmp, fmt.Sprintf("gojaccl-%s-metadata-%s", artifactDeviceName(r.opts.Device), r.opts.Stamp))
	r.ctlBin = filepath.Join(r.opts.Artifact, "jacclctl-"+r.short)
	r.proofBin = filepath.Join(r.opts.Artifact, "jacclproof-"+r.short)

	if err := r.writePreamble(); err != nil {
		return err
	}
	r.capture()
	failures := r.evaluate()
	if err := r.writeSummary(failures); err != nil {
		return err
	}
	tarPath, sum, err := r.packageArtifact()
	if err != nil {
		return err
	}
	if len(failures) > 0 {
		fmt.Fprintf(r.stdout, "metadata packet FAILED\nartifact preserved: %s\ntar: %s\nsha256: %s\nfailures:\n%s", r.opts.Artifact, tarPath, sum, strings.Join(failures, "\n"))
		if !strings.HasSuffix(strings.Join(failures, "\n"), "\n") {
			fmt.Fprintln(r.stdout)
		}
		return exitError{code: 1}
	}
	fmt.Fprintf(r.stdout, "metadata packet complete\nartifact: %s\ntar: %s\nsha256: %s\n\n", r.opts.Artifact, tarPath, sum)
	fmt.Fprintln(r.stdout, "This is metadata evidence only. Review status files and gid-summary.txt")
	fmt.Fprintln(r.stdout, "before considering any future RTR packet.")
	return nil
}

func (r *metadataRun) writePreamble() error {
	runEnv := []string{
		"ROOT=" + r.opts.Root,
		"REMOTE=" + r.opts.Remote,
		"REMOTE_TMP=" + r.opts.RemoteTmp,
		"DEVICE=" + r.opts.Device,
		fmt.Sprintf("MAX_GIDS=%d", r.opts.MaxGIDs),
		fmt.Sprintf("EXPECTED_SELECTED_GID_INDEX=%d", r.opts.ExpectedGIDIndex),
		fmt.Sprintf("TIMEOUT_SECONDS=%d", int(r.opts.Timeout/time.Second)),
		fmt.Sprintf("SSH_CONNECT_TIMEOUT=%d", int(r.opts.SSHTimeout/time.Second)),
		"ART=" + r.opts.Artifact,
		"REMOTE_ART=" + r.remoteArt,
		"HEAD=" + r.head,
		"SHORT=" + r.short,
	}
	if err := os.WriteFile(filepath.Join(r.opts.Artifact, "run.env"), []byte(strings.Join(runEnv, "\n")+"\n"), 0666); err != nil {
		return fmt.Errorf("write run.env: %w", err)
	}
	var readme strings.Builder
	fmt.Fprintf(&readme, "# gojaccl %s metadata packet\n\n", r.opts.Device)
	fmt.Fprintf(&readme, "- commit: %s\n", r.head)
	fmt.Fprintf(&readme, "- device: %s\n", r.opts.Device)
	fmt.Fprintf(&readme, "- max_gids: %d\n", r.opts.MaxGIDs)
	if r.opts.ExpectedGIDIndex >= 0 {
		fmt.Fprintf(&readme, "- expected_selected_gid_index: %d\n", r.opts.ExpectedGIDIndex)
	}
	fmt.Fprintf(&readme, "- timeout_seconds: %d\n", int(r.opts.Timeout/time.Second))
	fmt.Fprintf(&readme, "- local_artifact: %s\n", r.opts.Artifact)
	fmt.Fprintf(&readme, "- remote_artifact: %s\n\n", r.remoteArt)
	fmt.Fprintln(&readme, "This packet is metadata-only. It is not datapath proof and must not be")
	fmt.Fprintln(&readme, "used as an RDMA readiness claim or as permission to run RTR.")
	if err := os.WriteFile(filepath.Join(r.opts.Artifact, "README.md"), []byte(readme.String()), 0666); err != nil {
		return fmt.Errorf("write README: %w", err)
	}
	noRetry := fmt.Sprintf("NO_RETRY: one-shot %s metadata packet only. No RTR, no jaccld, no PD/MR/CQ/QP allocation, no work requests, no retry after timeout/nonzero/provider degradation.\n", r.opts.Device)
	if err := os.WriteFile(filepath.Join(r.opts.Artifact, "proof", "no-retry.txt"), []byte(noRetry), 0666); err != nil {
		return fmt.Errorf("write no-retry: %w", err)
	}
	return nil
}

func (r *metadataRun) capture() {
	proofDir := filepath.Join(r.opts.Artifact, "proof")
	localDir := filepath.Join(r.opts.Artifact, "local")
	remoteCtl := filepath.Join(r.remoteArt, "jacclctl-"+r.short)
	remoteProof := filepath.Join(r.remoteArt, "jacclproof-"+r.short)

	r.runCapture(proofDir, "build-jacclctl", 0, "go", "build", "-o", r.ctlBin, "./cmd/jacclctl")
	r.runCapture(proofDir, "build-jacclproof", 0, "go", "build", "-o", r.proofBin, "./cmd/jacclproof")
	r.runCapture(localDir, "jacclctl-sha256", 0, "shasum", "-a", "256", r.ctlBin)
	r.runCapture(localDir, "jacclproof-sha256", 0, "shasum", "-a", "256", r.proofBin)

	mkdir := fmt.Sprintf("mkdir -p %s", shellQuote(r.remoteArt))
	r.runCapture(proofDir, "remote-mkdir", 0, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout="+secondsString(r.opts.SSHTimeout), r.opts.Remote, mkdir)
	r.runCapture(proofDir, "copy-jacclctl", 0, "scp", "-o", "BatchMode=yes", "-o", "ConnectTimeout="+secondsString(r.opts.SSHTimeout), r.ctlBin, r.opts.Remote+":"+remoteCtl)
	r.runCapture(proofDir, "copy-jacclproof", 0, "scp", "-o", "BatchMode=yes", "-o", "ConnectTimeout="+secondsString(r.opts.SSHTimeout), r.proofBin, r.opts.Remote+":"+remoteProof)
	r.runCapture(proofDir, "remote-jacclctl-sha256", 0, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout="+secondsString(r.opts.SSHTimeout), r.opts.Remote, "shasum -a 256 "+shellQuote(remoteCtl))
	r.runCapture(proofDir, "remote-jacclproof-sha256", 0, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout="+secondsString(r.opts.SSHTimeout), r.opts.Remote, "shasum -a 256 "+shellQuote(remoteProof))

	r.runCapture(localDir, "preflight-rdma", r.opts.Timeout, "rdma_ctl", "status")
	r.runCapture(localDir, "preflight-ibv", r.opts.Timeout, "ibv_devinfo", "-d", r.opts.Device)
	r.runCapture(localDir, "preflight-ifconfig", 0, "ifconfig")
	r.runCapture(localDir, "preflight-netstat", 0, "netstat", "-rn")
	r.runCapture(localDir, "preflight-processes", 0, r.proofBin, "process-snapshot")

	r.remoteCapture("preflight-rdma", "rdma_ctl status", true)
	r.remoteCapture("preflight-ibv", "ibv_devinfo -d "+shellQuote(r.opts.Device), true)
	r.remoteCapture("preflight-ifconfig", "ifconfig", false)
	r.remoteCapture("preflight-netstat", "netstat -rn", false)
	r.remoteCapture("preflight-processes", shellQuote(remoteProof)+" process-snapshot", false)

	metaArgs := []string{"rdma-metadata", "-device", r.opts.Device, "-max-gids", strconv.Itoa(r.opts.MaxGIDs)}
	r.runCapture(localDir, "rdma-metadata-"+r.opts.Device, r.opts.Timeout, append([]string{r.ctlBin}, metaArgs...)...)
	r.remoteCapture("rdma-metadata-"+r.opts.Device, shellQuote(remoteCtl)+" "+joinShellArgs(metaArgs), true)

	r.runCapture(localDir, "postflight-rdma", r.opts.Timeout, "rdma_ctl", "status")
	r.runCapture(localDir, "postflight-ibv", r.opts.Timeout, "ibv_devinfo", "-d", r.opts.Device)
	r.runCapture(localDir, "postflight-processes", 0, r.proofBin, "process-snapshot")

	r.remoteCapture("postflight-rdma", "rdma_ctl status", true)
	r.remoteCapture("postflight-ibv", "ibv_devinfo -d "+shellQuote(r.opts.Device), true)
	r.remoteCapture("postflight-processes", shellQuote(remoteProof)+" process-snapshot", false)

	r.summarizeMetadata("local")
	r.summarizeMetadata("remote")
}

func (r *metadataRun) runCapture(dir, name string, timeout time.Duration, args ...string) {
	logCommand(filepath.Join(r.opts.Artifact, "proof", "commands.log"), args...)
	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Dir = r.opts.Root
	out, err := cmd.Output()
	status := 0
	errData := []byte(nil)
	if err != nil {
		status = commandStatus(err)
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			errData = ee.Stderr
		} else {
			errData = []byte(err.Error() + "\n")
		}
	}
	if ctx.Err() != nil {
		status = 124
		errData = append(errData, []byte(ctx.Err().Error()+"\n")...)
	}
	writeCapture(dir, name, out, errData, status)
}

func (r *metadataRun) remoteCapture(name, remoteCommand string, timeout bool) {
	mkdir := "mkdir -p " + shellQuote(filepath.Join(r.remoteArt, "remote")) + " " + shellQuote(filepath.Join(r.remoteArt, "proof"))
	cmd := mkdir + " && { " + remoteCommand + "; }"
	if timeout {
		cmd = "perl -e 'alarm shift; exec @ARGV' " + strconv.Itoa(int(r.opts.Timeout/time.Second)) + " sh -c " + shellQuote(cmd)
	}
	args := []string{"ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=" + secondsString(r.opts.SSHTimeout), r.opts.Remote, cmd}
	logRemoteCommand(filepath.Join(r.opts.Artifact, "proof", "commands.log"), r.opts.Remote, cmd)
	r.runCaptureNoLog(filepath.Join(r.opts.Artifact, "remote"), name, args...)
}

func (r *metadataRun) runCaptureNoLog(dir, name string, args ...string) {
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = r.opts.Root
	out, err := cmd.Output()
	status := 0
	errData := []byte(nil)
	if err != nil {
		status = commandStatus(err)
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			errData = ee.Stderr
		} else {
			errData = []byte(err.Error() + "\n")
		}
	}
	writeCapture(dir, name, out, errData, status)
}

func writeCapture(dir, name string, out, errData []byte, status int) {
	_ = os.WriteFile(filepath.Join(dir, name+".out"), out, 0666)
	_ = os.WriteFile(filepath.Join(dir, name+".err"), errData, 0666)
	_ = os.WriteFile(filepath.Join(dir, name+".status"), []byte(strconv.Itoa(status)+"\n"), 0666)
}

func (r *metadataRun) evaluate() []string {
	var failures []string
	add := func(s string) { failures = append(failures, s) }
	proofDir := filepath.Join(r.opts.Artifact, "proof")
	localDir := filepath.Join(r.opts.Artifact, "local")
	remoteDir := filepath.Join(r.opts.Artifact, "remote")

	requireStatusZero(&failures, "local jacclctl build", filepath.Join(proofDir, "build-jacclctl.status"))
	requireStatusZero(&failures, "local jacclproof build", filepath.Join(proofDir, "build-jacclproof.status"))
	requireStatusZero(&failures, "local jacclctl sha256", filepath.Join(localDir, "jacclctl-sha256.status"))
	requireStatusZero(&failures, "local jacclproof sha256", filepath.Join(localDir, "jacclproof-sha256.status"))
	requireStatusZero(&failures, "remote mkdir", filepath.Join(proofDir, "remote-mkdir.status"))
	requireStatusZero(&failures, "copy jacclctl to remote", filepath.Join(proofDir, "copy-jacclctl.status"))
	requireStatusZero(&failures, "copy jacclproof to remote", filepath.Join(proofDir, "copy-jacclproof.status"))
	requireStatusZero(&failures, "remote jacclctl sha256", filepath.Join(proofDir, "remote-jacclctl-sha256.status"))
	requireStatusZero(&failures, "remote jacclproof sha256", filepath.Join(proofDir, "remote-jacclproof-sha256.status"))
	if !isExecutable(r.ctlBin) {
		add("local jacclctl binary is missing or not executable")
	}
	if !isExecutable(r.proofBin) {
		add("local jacclproof binary is missing or not executable")
	}
	requireEqualHash(&failures, "jacclctl", filepath.Join(localDir, "jacclctl-sha256.out"), filepath.Join(proofDir, "remote-jacclctl-sha256.out"))
	requireEqualHash(&failures, "jacclproof", filepath.Join(localDir, "jacclproof-sha256.out"), filepath.Join(proofDir, "remote-jacclproof-sha256.out"))

	for _, side := range []struct {
		name string
		dir  string
	}{
		{"local", localDir},
		{"remote", remoteDir},
	} {
		requireStatusZero(&failures, side.name+" preflight rdma", filepath.Join(side.dir, "preflight-rdma.status"))
		requireStatusZero(&failures, side.name+" preflight ibv", filepath.Join(side.dir, "preflight-ibv.status"))
		requireStatusZero(&failures, side.name+" metadata", filepath.Join(side.dir, "rdma-metadata-"+r.opts.Device+".status"))
		requireStatusZero(&failures, side.name+" postflight rdma", filepath.Join(side.dir, "postflight-rdma.status"))
		requireStatusZero(&failures, side.name+" postflight ibv", filepath.Join(side.dir, "postflight-ibv.status"))
		switch r.profile.rdmaStatusMode {
		case "enabled":
			requireContains(&failures, side.name+" preflight rdma", filepath.Join(side.dir, "preflight-rdma.out"), "enabled")
			requireContains(&failures, side.name+" postflight rdma", filepath.Join(side.dir, "postflight-rdma.out"), "enabled")
		case "device-active":
			requireLineContains(&failures, side.name+" preflight rdma", filepath.Join(side.dir, "preflight-rdma.out"), r.opts.Device, "PORT_ACTIVE")
			requireLineContains(&failures, side.name+" postflight rdma", filepath.Join(side.dir, "postflight-rdma.out"), r.opts.Device, "PORT_ACTIVE")
		}
		requireContains(&failures, side.name+" preflight ibv", filepath.Join(side.dir, "preflight-ibv.out"), r.opts.Device)
		requireContains(&failures, side.name+" postflight ibv", filepath.Join(side.dir, "postflight-ibv.out"), r.opts.Device)
		requireContains(&failures, side.name+" preflight ibv", filepath.Join(side.dir, "preflight-ibv.out"), "PORT_ACTIVE")
		requireContains(&failures, side.name+" postflight ibv", filepath.Join(side.dir, "postflight-ibv.out"), "PORT_ACTIVE")
		meta := filepath.Join(side.dir, "rdma-metadata-"+r.opts.Device+".out")
		requireContains(&failures, side.name+" metadata", meta, "rdma metadata device="+r.opts.Device)
		requireContains(&failures, side.name+" metadata", meta, fmt.Sprintf("gid_scan_limit=%d", r.opts.MaxGIDs))
		if r.opts.ExpectedGIDIndex >= 0 {
			requireContains(&failures, side.name+" metadata", meta, fmt.Sprintf("selected_gid_index=%d", r.opts.ExpectedGIDIndex))
			requireLineContains(&failures, side.name+" metadata", meta, fmt.Sprintf("gid index=%d ", r.opts.ExpectedGIDIndex), "zero=false")
		}
		requireEmptyProcessSnapshot(&failures, side.name+" preflight processes", filepath.Join(side.dir, "preflight-processes.out"), filepath.Join(side.dir, "preflight-processes.status"))
		requireEmptyProcessSnapshot(&failures, side.name+" postflight processes", filepath.Join(side.dir, "postflight-processes.out"), filepath.Join(side.dir, "postflight-processes.status"))
	}
	if len(failures) > 0 {
		_ = os.WriteFile(filepath.Join(proofDir, "failures.txt"), []byte(strings.Join(failures, "\n")+"\n"), 0666)
	} else {
		_ = os.Remove(filepath.Join(proofDir, "failures.txt"))
	}
	return failures
}

func (r *metadataRun) writeSummary(failures []string) error {
	state := "passed"
	if len(failures) > 0 {
		state = "failed"
	}
	summary := struct {
		State            string `json:"state"`
		Commit           string `json:"commit"`
		Device           string `json:"device"`
		MaxGIDs          int    `json:"max_gids"`
		ExpectedGIDIndex int    `json:"expected_selected_gid_index,omitempty"`
		TimeoutSeconds   int    `json:"timeout_seconds"`
		Artifact         string `json:"artifact"`
	}{
		State:            state,
		Commit:           r.head,
		Device:           r.opts.Device,
		MaxGIDs:          r.opts.MaxGIDs,
		ExpectedGIDIndex: r.opts.ExpectedGIDIndex,
		TimeoutSeconds:   int(r.opts.Timeout / time.Second),
		Artifact:         r.opts.Artifact,
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(r.opts.Artifact, "proof", "summary.json"), data, 0666)
}

func (r *metadataRun) packageArtifact() (string, string, error) {
	manifest, err := r.artifactManifest()
	if err != nil {
		return "", "", err
	}
	if err := os.WriteFile(filepath.Join(r.opts.Artifact, "artifact-manifest.txt"), []byte(strings.Join(manifest, "\n")+"\n"), 0666); err != nil {
		return "", "", fmt.Errorf("write artifact manifest: %w", err)
	}
	tarPath := r.opts.Artifact + ".tar.gz"
	if err := writeTarGz(tarPath, r.opts.Artifact); err != nil {
		return "", "", err
	}
	sum, err := fileSHA256(tarPath)
	if err != nil {
		return "", "", err
	}
	if err := os.WriteFile(filepath.Join(r.opts.Artifact, "tar-sha256.txt"), []byte(sum+"  "+tarPath+"\n"), 0666); err != nil {
		return "", "", fmt.Errorf("write tar sha256: %w", err)
	}
	return tarPath, sum, nil
}

func (r *metadataRun) artifactManifest() ([]string, error) {
	var files []string
	err := filepath.WalkDir(r.opts.Artifact, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func (r *metadataRun) summarizeMetadata(side string) {
	out := filepath.Join(r.opts.Artifact, side, "rdma-metadata-"+r.opts.Device+".out")
	data, err := os.ReadFile(out)
	if err != nil || len(data) == 0 {
		return
	}
	var b strings.Builder
	lines := strings.Split(string(data), "\n")
	nonzero, mapped := 0, 0
	for _, line := range lines {
		if strings.HasPrefix(line, "rdma metadata ") {
			fmt.Fprintln(&b, line)
		}
		if strings.Contains(line, "zero=false") {
			nonzero++
		}
		if strings.Contains(line, "ipv4_mapped=true") {
			mapped++
		}
	}
	fmt.Fprintf(&b, "nonzero_gid_count=%d\n", nonzero)
	fmt.Fprintf(&b, "ipv4_mapped_gid_count=%d\n", mapped)
	for _, line := range lines {
		if strings.Contains(line, "zero=false") || strings.Contains(line, "ipv4_mapped=true") {
			fmt.Fprintln(&b, line)
		}
	}
	_ = os.WriteFile(filepath.Join(r.opts.Artifact, side, "gid-summary.txt"), []byte(b.String()), 0666)
}

func commandOutput(dir, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out)), nil
}

func commandStatus(err error) int {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return 1
}

func readStatus(path string) string {
	data, err := os.ReadFile(path)
	if err != nil || len(strings.TrimSpace(string(data))) == 0 {
		return "missing"
	}
	return strings.TrimSpace(string(data))
}

func requireStatusZero(failures *[]string, label, path string) {
	if status := readStatus(path); status != "0" {
		*failures = append(*failures, fmt.Sprintf("%s status=%s", label, status))
	}
}

func requireContains(failures *[]string, label, path, pattern string) {
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), pattern) {
		*failures = append(*failures, fmt.Sprintf("%s missing pattern: %s", label, pattern))
	}
}

func requireLineContains(failures *[]string, label, path, a, b string) {
	data, err := os.ReadFile(path)
	if err != nil {
		*failures = append(*failures, fmt.Sprintf("%s missing line with %s and %s", label, a, b))
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(line, a) && strings.Contains(line, b) {
			return
		}
	}
	*failures = append(*failures, fmt.Sprintf("%s missing line with %s and %s", label, a, b))
}

func requireEmptyProcessSnapshot(failures *[]string, label, outPath, statusPath string) {
	switch status := readStatus(statusPath); status {
	case "0":
		*failures = append(*failures, label+" found stale processes")
	case "1":
	default:
		*failures = append(*failures, fmt.Sprintf("%s process snapshot status=%s", label, status))
	}
	data, err := os.ReadFile(outPath)
	if err == nil && len(data) > 0 {
		*failures = append(*failures, label+" process snapshot output is not empty")
	}
}

func requireEqualHash(failures *[]string, label, localPath, remotePath string) {
	local := firstField(localPath)
	remote := firstField(remotePath)
	if local == "" || remote == "" || local != remote {
		*failures = append(*failures, label+" local and remote sha256 files differ")
	}
}

func firstField(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func isExecutable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Mode()&0111 != 0
}

func logCommand(path string, args ...string) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintln(f, "+ "+joinShellArgs(args))
}

func logRemoteCommand(path, remote, cmd string) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "+ ssh %s %s\n", shellQuote(remote), shellQuote(cmd))
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if strings.IndexFunc(s, func(r rune) bool {
		return !(r == '_' || r == '-' || r == '.' || r == '/' || r == ':' || r == '@' || r == '=' || r == '+' || r >= '0' && r <= '9' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z')
	}) < 0 {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

func joinShellArgs(args []string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = shellQuote(arg)
	}
	return strings.Join(quoted, " ")
}

func secondsString(d time.Duration) string {
	return strconv.Itoa(int(d / time.Second))
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func artifactDeviceName(device string) string {
	return strings.ReplaceAll(device, "_", "-")
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getenvDurationSeconds(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return time.Duration(n) * time.Second
}

func writeTarGz(path, root string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create tar: %w", err)
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()
	base := filepath.Base(root)
	parent := filepath.Dir(root)
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(parent, path)
		if err != nil {
			return err
		}
		if rel == "." {
			rel = base
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		_, err = io.Copy(tw, in)
		return err
	})
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
