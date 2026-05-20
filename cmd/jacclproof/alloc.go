package main

import (
	"context"
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

type allocOptions struct {
	Command      string
	Device       string
	RemoteDevice string
	Remote       string
	RemoteTmp    string
	Root         string
	Artifact     string
	Stamp        string
	CQCapacity   int
	MRBytes      int
	InitQP       bool
	Timeout      time.Duration
	SSHTimeout   time.Duration
}

type allocRun struct {
	opts   allocOptions
	stdout io.Writer

	head      string
	short     string
	remoteArt string
	ctlBin    string
	proofBin  string
}

func runRDMAAllocPacket(args []string, stdout, stderr io.Writer) error {
	opts, err := parseAllocOptions("rdma-alloc", false, args, stderr)
	if err != nil {
		return err
	}
	if err := validateAllocOptions(opts); err != nil {
		return exitError{code: 2, err: err}
	}
	r := allocRun{opts: opts, stdout: stdout}
	return r.run()
}

func runRDMAInitPacket(args []string, stdout, stderr io.Writer) error {
	opts, err := parseAllocOptions("rdma-init", true, args, stderr)
	if err != nil {
		return err
	}
	if err := validateAllocOptions(opts); err != nil {
		return exitError{code: 2, err: err}
	}
	r := allocRun{opts: opts, stdout: stdout}
	return r.run()
}

func parseAllocOptions(command string, initQP bool, args []string, stderr io.Writer) (allocOptions, error) {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	device := fs.String("device", getenv("DEVICE", "rdma_en1"), "RDMA device")
	remoteDevice := fs.String("remote-device", getenv("REMOTE_DEVICE", ""), "remote RDMA device; defaults to -device")
	remote := fs.String("remote", os.Getenv("REMOTE"), "peer SSH target")
	remoteTmp := fs.String("remote-tmp", os.Getenv("REMOTE_TMP"), "peer writable artifact directory")
	root := fs.String("root", os.Getenv("ROOT"), "repository root")
	artifact := fs.String("art", os.Getenv("ART"), "local artifact directory")
	stamp := fs.String("stamp", os.Getenv("STAMP"), "UTC artifact stamp")
	cqCapacity := fs.Int("cq-capacity", getenvInt("CQ_CAPACITY", 4), "completion queue capacity")
	mrBytes := fs.Int("mr-bytes", getenvInt("MR_BYTES", 4096), "mmap-backed memory region size")
	timeout := fs.Duration("timeout", getenvDurationSeconds("TIMEOUT_SECONDS", 0), "per-command timeout")
	sshTimeout := fs.Duration("ssh-connect-timeout", getenvDurationSeconds("SSH_CONNECT_TIMEOUT", 10*time.Second), "SSH connect timeout")
	if err := fs.Parse(args); err != nil {
		return allocOptions{}, exitError{code: 2, err: err}
	}
	if fs.NArg() != 0 {
		return allocOptions{}, exitError{code: 2, err: fmt.Errorf("unexpected %s arguments", command)}
	}
	profile, err := metadataProfileForDevice(*device)
	if err != nil {
		return allocOptions{}, exitError{code: 2, err: err}
	}
	if *remoteDevice == "" {
		*remoteDevice = *device
	}
	if _, err := metadataProfileForDevice(*remoteDevice); err != nil {
		return allocOptions{}, exitError{code: 2, err: fmt.Errorf("remote-device: %w", err)}
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
			return allocOptions{}, fmt.Errorf("getwd: %w", err)
		}
		*root = wd
	}
	if *artifact == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return allocOptions{}, fmt.Errorf("home directory: %w", err)
		}
		kind := strings.TrimPrefix(command, "rdma-")
		*artifact = filepath.Join(home, "tmp", fmt.Sprintf("gojaccl-%s-%s-%s", artifactDeviceName(*device), kind, *stamp))
	}
	return allocOptions{
		Command:      command,
		Device:       *device,
		RemoteDevice: *remoteDevice,
		Remote:       *remote,
		RemoteTmp:    *remoteTmp,
		Root:         *root,
		Artifact:     *artifact,
		Stamp:        *stamp,
		CQCapacity:   *cqCapacity,
		MRBytes:      *mrBytes,
		InitQP:       initQP,
		Timeout:      *timeout,
		SSHTimeout:   *sshTimeout,
	}, nil
}

func validateAllocOptions(opts allocOptions) error {
	if opts.Remote == "" {
		return fmt.Errorf("remote is required")
	}
	if opts.RemoteTmp == "" {
		return fmt.Errorf("remote-tmp is required")
	}
	if opts.CQCapacity <= 0 {
		return fmt.Errorf("cq-capacity %d must be positive", opts.CQCapacity)
	}
	if opts.MRBytes <= 0 {
		return fmt.Errorf("mr-bytes %d must be positive", opts.MRBytes)
	}
	if opts.Timeout <= 0 {
		return fmt.Errorf("timeout %s must be positive", opts.Timeout)
	}
	if opts.SSHTimeout <= 0 {
		return fmt.Errorf("ssh-connect-timeout %s must be positive", opts.SSHTimeout)
	}
	return nil
}

func (r *allocRun) run() error {
	for _, dir := range []string{"local", "remote", "proof"} {
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
	kind := strings.TrimPrefix(r.opts.Command, "rdma-")
	r.remoteArt = filepath.Join(r.opts.RemoteTmp, fmt.Sprintf("gojaccl-%s-%s-%s", artifactDeviceName(r.opts.Device), kind, r.opts.Stamp))
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
		fmt.Fprintf(r.stdout, "%s packet FAILED\nartifact preserved: %s\ntar: %s\nsha256: %s\nfailures:\n%s", kind, r.opts.Artifact, tarPath, sum, strings.Join(failures, "\n"))
		if !strings.HasSuffix(strings.Join(failures, "\n"), "\n") {
			fmt.Fprintln(r.stdout)
		}
		return exitError{code: 1}
	}
	fmt.Fprintf(r.stdout, "%s packet complete\nartifact: %s\ntar: %s\nsha256: %s\n\n", kind, r.opts.Artifact, tarPath, sum)
	if r.opts.InitQP {
		fmt.Fprintln(r.stdout, "This proves resource allocation, QP INIT, and teardown only.")
	} else {
		fmt.Fprintln(r.stdout, "This proves resource allocation and teardown only.")
	}
	fmt.Fprintln(r.stdout, "It does not authorize RTR or datapath work requests.")
	return nil
}

func (r *allocRun) writePreamble() error {
	runEnv := []string{
		"ROOT=" + r.opts.Root,
		"REMOTE=" + r.opts.Remote,
		"REMOTE_TMP=" + r.opts.RemoteTmp,
		"DEVICE=" + r.opts.Device,
		"REMOTE_DEVICE=" + r.opts.RemoteDevice,
		"COMMAND=" + r.opts.Command,
		fmt.Sprintf("CQ_CAPACITY=%d", r.opts.CQCapacity),
		fmt.Sprintf("MR_BYTES=%d", r.opts.MRBytes),
		fmt.Sprintf("INIT_QP=%t", r.opts.InitQP),
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
	kind := strings.TrimPrefix(r.opts.Command, "rdma-")
	fmt.Fprintf(&readme, "# gojaccl %s %s packet\n\n", r.opts.Device, kind)
	fmt.Fprintf(&readme, "- commit: %s\n", r.head)
	fmt.Fprintf(&readme, "- command: %s\n", r.opts.Command)
	fmt.Fprintf(&readme, "- device: %s\n", r.opts.Device)
	fmt.Fprintf(&readme, "- remote_device: %s\n", r.opts.RemoteDevice)
	fmt.Fprintf(&readme, "- cq_capacity: %d\n", r.opts.CQCapacity)
	fmt.Fprintf(&readme, "- mr_bytes: %d\n", r.opts.MRBytes)
	fmt.Fprintf(&readme, "- init_qp: %t\n", r.opts.InitQP)
	fmt.Fprintf(&readme, "- timeout_seconds: %d\n", int(r.opts.Timeout/time.Second))
	fmt.Fprintf(&readme, "- local_artifact: %s\n", r.opts.Artifact)
	fmt.Fprintf(&readme, "- remote_artifact: %s\n\n", r.remoteArt)
	if r.opts.InitQP {
		fmt.Fprintln(&readme, "This packet allocates PD/MR/CQ/QP resources, moves the QP to INIT, and tears everything down.")
	} else {
		fmt.Fprintln(&readme, "This packet allocates and tears down PD/MR/CQ/QP resources only.")
	}
	fmt.Fprintln(&readme, "It must not be used as RTR or datapath proof.")
	if err := os.WriteFile(filepath.Join(r.opts.Artifact, "README.md"), []byte(readme.String()), 0666); err != nil {
		return fmt.Errorf("write README: %w", err)
	}
	noRTR := fmt.Sprintf("NO_RTR: %s %s packet only. No RTR, no RTS, no work requests, no jaccld, no retry after timeout/nonzero/provider degradation.\n", r.opts.Device, kind)
	if err := os.WriteFile(filepath.Join(r.opts.Artifact, "proof", "no-rtr.txt"), []byte(noRTR), 0666); err != nil {
		return fmt.Errorf("write no-rtr: %w", err)
	}
	return nil
}

func (r *allocRun) capture() {
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
	r.runCapture(localDir, "preflight-processes", 0, r.proofBin, "process-snapshot")
	r.remoteCapture("preflight-rdma", "rdma_ctl status", true)
	r.remoteCapture("preflight-ibv", "ibv_devinfo -d "+shellQuote(r.opts.RemoteDevice), true)
	r.remoteCapture("preflight-processes", shellQuote(remoteProof)+" process-snapshot", false)

	allocArgs := []string{
		r.opts.Command,
		"-device", r.opts.Device,
		"-cq-capacity", strconv.Itoa(r.opts.CQCapacity),
		"-mr-bytes", strconv.Itoa(r.opts.MRBytes),
	}
	r.runCapture(localDir, r.opts.Command+"-"+r.opts.Device, r.opts.Timeout, append([]string{r.ctlBin}, allocArgs...)...)
	remoteAllocArgs := []string{
		r.opts.Command,
		"-device", r.opts.RemoteDevice,
		"-cq-capacity", strconv.Itoa(r.opts.CQCapacity),
		"-mr-bytes", strconv.Itoa(r.opts.MRBytes),
	}
	r.remoteCapture(r.opts.Command+"-"+r.opts.RemoteDevice, shellQuote(remoteCtl)+" "+joinShellArgs(remoteAllocArgs), true)

	r.runCapture(localDir, "postflight-rdma", r.opts.Timeout, "rdma_ctl", "status")
	r.runCapture(localDir, "postflight-ibv", r.opts.Timeout, "ibv_devinfo", "-d", r.opts.Device)
	r.runCapture(localDir, "postflight-processes", 0, r.proofBin, "process-snapshot")
	r.remoteCapture("postflight-rdma", "rdma_ctl status", true)
	r.remoteCapture("postflight-ibv", "ibv_devinfo -d "+shellQuote(r.opts.RemoteDevice), true)
	r.remoteCapture("postflight-processes", shellQuote(remoteProof)+" process-snapshot", false)
}

func (r *allocRun) runCapture(dir, name string, timeout time.Duration, args ...string) {
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

func (r *allocRun) remoteCapture(name, remoteCommand string, timeout bool) {
	mkdir := "mkdir -p " + shellQuote(filepath.Join(r.remoteArt, "remote")) + " " + shellQuote(filepath.Join(r.remoteArt, "proof"))
	cmd := mkdir + " && { " + remoteCommand + "; }"
	if timeout {
		cmd = "perl -e 'alarm shift; exec @ARGV' " + strconv.Itoa(int(r.opts.Timeout/time.Second)) + " sh -c " + shellQuote(cmd)
	}
	args := []string{"ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=" + secondsString(r.opts.SSHTimeout), r.opts.Remote, cmd}
	logRemoteCommand(filepath.Join(r.opts.Artifact, "proof", "commands.log"), r.opts.Remote, cmd)
	r.runCaptureNoLog(filepath.Join(r.opts.Artifact, "remote"), name, args...)
}

func (r *allocRun) runCaptureNoLog(dir, name string, args ...string) {
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

func (r *allocRun) evaluate() []string {
	var failures []string
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
		failures = append(failures, "local jacclctl binary is missing or not executable")
	}
	if !isExecutable(r.proofBin) {
		failures = append(failures, "local jacclproof binary is missing or not executable")
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
		device := r.opts.Device
		if side.name == "remote" {
			device = r.opts.RemoteDevice
		}
		alloc := filepath.Join(side.dir, r.opts.Command+"-"+device+".out")
		requireStatusZero(&failures, side.name+" preflight rdma", filepath.Join(side.dir, "preflight-rdma.status"))
		requireStatusZero(&failures, side.name+" preflight ibv", filepath.Join(side.dir, "preflight-ibv.status"))
		requireStatusZero(&failures, side.name+" allocation", filepath.Join(side.dir, r.opts.Command+"-"+device+".status"))
		requireStatusZero(&failures, side.name+" postflight rdma", filepath.Join(side.dir, "postflight-rdma.status"))
		requireStatusZero(&failures, side.name+" postflight ibv", filepath.Join(side.dir, "postflight-ibv.status"))
		requireContains(&failures, side.name+" preflight rdma", filepath.Join(side.dir, "preflight-rdma.out"), "enabled")
		requireContains(&failures, side.name+" postflight rdma", filepath.Join(side.dir, "postflight-rdma.out"), "enabled")
		requireContains(&failures, side.name+" preflight ibv", filepath.Join(side.dir, "preflight-ibv.out"), device)
		requireContains(&failures, side.name+" postflight ibv", filepath.Join(side.dir, "postflight-ibv.out"), device)
		requireContains(&failures, side.name+" preflight ibv", filepath.Join(side.dir, "preflight-ibv.out"), "PORT_ACTIVE")
		requireContains(&failures, side.name+" postflight ibv", filepath.Join(side.dir, "postflight-ibv.out"), "PORT_ACTIVE")
		requireContains(&failures, side.name+" allocation", alloc, "rdma resource command="+r.opts.Command)
		requireContains(&failures, side.name+" allocation", alloc, "device="+device)
		requireContains(&failures, side.name+" allocation", alloc, fmt.Sprintf("cq_capacity=%d", r.opts.CQCapacity))
		requireContains(&failures, side.name+" allocation", alloc, fmt.Sprintf("mr_bytes=%d", r.opts.MRBytes))
		requireContains(&failures, side.name+" allocation", alloc, "qpn_nonzero=true")
		requireContains(&failures, side.name+" allocation", alloc, "addr_nonzero=true")
		requireContains(&failures, side.name+" allocation", alloc, "lkey_nonzero=true")
		requireContains(&failures, side.name+" allocation", alloc, "rkey_nonzero=")
		requireContains(&failures, side.name+" allocation", alloc, fmt.Sprintf("init=%t", r.opts.InitQP))
		requireContains(&failures, side.name+" allocation", alloc, "rtr=false")
		requireContains(&failures, side.name+" allocation", alloc, "work_requests=false")
		for _, resource := range []string{"protection_domain", "completion_queue", "queue_pair", "memory_region"} {
			requireContains(&failures, side.name+" allocation", alloc, "resource "+resource+"=allocated")
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

func (r *allocRun) writeSummary(failures []string) error {
	state := "passed"
	if len(failures) > 0 {
		state = "failed"
	}
	summary := struct {
		State          string `json:"state"`
		Commit         string `json:"commit"`
		Device         string `json:"device"`
		RemoteDevice   string `json:"remote_device"`
		Command        string `json:"command"`
		CQCapacity     int    `json:"cq_capacity"`
		MRBytes        int    `json:"mr_bytes"`
		InitQP         bool   `json:"init_qp"`
		TimeoutSeconds int    `json:"timeout_seconds"`
		Artifact       string `json:"artifact"`
	}{
		State:          state,
		Commit:         r.head,
		Device:         r.opts.Device,
		RemoteDevice:   r.opts.RemoteDevice,
		Command:        r.opts.Command,
		CQCapacity:     r.opts.CQCapacity,
		MRBytes:        r.opts.MRBytes,
		InitQP:         r.opts.InitQP,
		TimeoutSeconds: int(r.opts.Timeout / time.Second),
		Artifact:       r.opts.Artifact,
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(r.opts.Artifact, "proof", "summary.json"), data, 0666)
}

func (r *allocRun) packageArtifact() (string, string, error) {
	manifest, err := artifactManifest(r.opts.Artifact)
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

func artifactManifest(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
