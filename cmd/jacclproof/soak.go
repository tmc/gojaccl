package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type soakOptions struct {
	Device           string
	RemoteDevice     string
	Remote           string
	RemoteTmp        string
	Root             string
	Artifact         string
	Stamp            string
	LocalRDMAIP      string
	RemoteRDMAIP     string
	LocalRouteIface  string
	RemoteRouteIface string
	CoordinatorPort  int
	SoakSeconds      int
	SoakInterval     int
	MaintainTimeout  time.Duration
	CommandTimeout   time.Duration
	SSHTimeout       time.Duration
	ExpectedGIDIndex int
}

type soakRun struct {
	opts        soakOptions
	stdout      io.Writer
	head        string
	remoteArt   string
	coordinator string
	packaged    bool
	started     bool
}

func runRDMASoakPacket(args []string, stdout, stderr io.Writer) error {
	opts, err := parseSoakOptions(args, stderr)
	if err != nil {
		return err
	}
	if os.Getenv("CONFIRM_RDMA_EN1_SOAK_ONE_SHOT") != "one-shot-soak" {
		fmt.Fprint(stderr, rdmaEn1SoakRefusal)
		return exitError{code: 2}
	}
	if err := validateSoakOptions(opts); err != nil {
		return exitError{code: 2, err: err}
	}
	r := soakRun{opts: opts, stdout: stdout}
	return r.run()
}

func parseSoakOptions(args []string, stderr io.Writer) (soakOptions, error) {
	fs := flag.NewFlagSet("rdma-soak", flag.ContinueOnError)
	fs.SetOutput(stderr)
	device := fs.String("device", getenv("DEVICE", "rdma_en1"), "RDMA device")
	remoteDevice := fs.String("remote-device", getenv("REMOTE_DEVICE", ""), "remote RDMA device; defaults to -device")
	remote := fs.String("remote", os.Getenv("REMOTE"), "peer SSH target")
	remoteTmp := fs.String("remote-tmp", os.Getenv("REMOTE_TMP"), "peer writable artifact directory")
	root := fs.String("root", os.Getenv("ROOT"), "repository root")
	artifact := fs.String("art", os.Getenv("ART"), "local artifact directory")
	stamp := fs.String("stamp", os.Getenv("STAMP"), "UTC artifact stamp")
	localIP := fs.String("local-rdma-ip", os.Getenv("LOCAL_RDMA_IP"), "local RDMA tcpchan address")
	remoteIP := fs.String("remote-rdma-ip", os.Getenv("REMOTE_RDMA_IP"), "remote RDMA tcpchan address")
	localRouteIface := fs.String("local-route-interface", getenv("LOCAL_ROUTE_INTERFACE", "en1"), "required local route interface")
	remoteRouteIface := fs.String("remote-route-interface", getenv("REMOTE_ROUTE_INTERFACE", "en1"), "required remote route interface")
	coordPort := fs.Int("coord-port", getenvInt("COORD_PORT", 39311), "coordinator TCP port")
	soakSeconds := fs.Int("soak-seconds", getenvInt("SOAK_SECONDS", 7200), "soak duration in seconds")
	interval := fs.Int("soak-interval", getenvInt("SOAK_INTERVAL_SECONDS", 60), "maintenance interval in seconds")
	maintainTimeout := fs.Duration("maintain-timeout", getenvDuration("MAINTAIN_TIMEOUT", 5*time.Second), "jacclctl maintain timeout")
	commandTimeout := fs.Duration("command-timeout", getenvDurationSeconds("COMMAND_TIMEOUT_SECONDS", 60*time.Second), "command timeout")
	sshTimeout := fs.Duration("ssh-connect-timeout", getenvDurationSeconds("SSH_CONNECT_TIMEOUT", 8*time.Second), "SSH connect timeout")
	expected := fs.Int("expected-selected-gid-index", getenvInt("EXPECTED_SELECTED_GID_INDEX", 1), "required selected GID index")
	if err := fs.Parse(args); err != nil {
		return soakOptions{}, exitError{code: 2, err: err}
	}
	if fs.NArg() != 0 {
		return soakOptions{}, exitError{code: 2, err: fmt.Errorf("unexpected rdma-soak arguments")}
	}
	if *remoteDevice == "" {
		*remoteDevice = *device
	}
	if *stamp == "" {
		*stamp = time.Now().UTC().Format("20060102T150405Z")
	}
	if *root == "" {
		wd, err := os.Getwd()
		if err != nil {
			return soakOptions{}, fmt.Errorf("getwd: %w", err)
		}
		*root = wd
	}
	if *artifact == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return soakOptions{}, fmt.Errorf("home directory: %w", err)
		}
		*artifact = filepath.Join(home, "tmp", fmt.Sprintf("gojaccl-%s-soak-%s", artifactDeviceName(*device), *stamp))
	}
	return soakOptions{
		Device:           *device,
		RemoteDevice:     *remoteDevice,
		Remote:           *remote,
		RemoteTmp:        *remoteTmp,
		Root:             *root,
		Artifact:         *artifact,
		Stamp:            *stamp,
		LocalRDMAIP:      *localIP,
		RemoteRDMAIP:     *remoteIP,
		LocalRouteIface:  *localRouteIface,
		RemoteRouteIface: *remoteRouteIface,
		CoordinatorPort:  *coordPort,
		SoakSeconds:      *soakSeconds,
		SoakInterval:     *interval,
		MaintainTimeout:  *maintainTimeout,
		CommandTimeout:   *commandTimeout,
		SSHTimeout:       *sshTimeout,
		ExpectedGIDIndex: *expected,
	}, nil
}

func validateSoakOptions(opts soakOptions) error {
	if _, err := metadataProfileForDevice(opts.Device); err != nil {
		return err
	}
	if _, err := metadataProfileForDevice(opts.RemoteDevice); err != nil {
		return fmt.Errorf("remote-device: %w", err)
	}
	if opts.Remote == "" {
		return fmt.Errorf("remote is required")
	}
	if opts.RemoteTmp == "" {
		return fmt.Errorf("remote-tmp is required")
	}
	if opts.LocalRDMAIP == "" {
		return fmt.Errorf("local-rdma-ip is required")
	}
	if opts.RemoteRDMAIP == "" {
		return fmt.Errorf("remote-rdma-ip is required")
	}
	if strings.TrimSpace(opts.LocalRouteIface) == "" {
		return fmt.Errorf("local-route-interface is required")
	}
	if strings.TrimSpace(opts.RemoteRouteIface) == "" {
		return fmt.Errorf("remote-route-interface is required")
	}
	if opts.CoordinatorPort <= 0 || opts.CoordinatorPort > 65535 {
		return fmt.Errorf("coord-port %d out of range", opts.CoordinatorPort)
	}
	if opts.SoakSeconds < 7200 {
		return fmt.Errorf("soak-seconds must be at least 7200")
	}
	if opts.SoakInterval != 60 {
		return fmt.Errorf("soak-interval must remain 60 for this reviewed packet")
	}
	if opts.MaintainTimeout <= 0 {
		return fmt.Errorf("maintain-timeout %s must be positive", opts.MaintainTimeout)
	}
	if opts.CommandTimeout <= 0 {
		return fmt.Errorf("command-timeout %s must be positive", opts.CommandTimeout)
	}
	if opts.SSHTimeout <= 0 {
		return fmt.Errorf("ssh-connect-timeout %s must be positive", opts.SSHTimeout)
	}
	if opts.ExpectedGIDIndex < 0 {
		return fmt.Errorf("expected-selected-gid-index must be non-negative")
	}
	return nil
}

func (r *soakRun) run() (err error) {
	if err := r.init(); err != nil {
		return err
	}
	defer func() {
		status := 0
		if err != nil {
			status = 1
		}
		r.onExit(status)
	}()
	steps := []func() error{
		r.runSafeGates,
		r.buildAndCopy,
		r.preflight,
		r.tcpdiag,
		r.writeWrappers,
		r.launchDaemons,
		r.postIPCLivenessGate,
		func() error { return r.smokePair("pre-soak") },
		func() error { return r.captureStats("after-pre-smoke") },
		r.maintenanceWindow,
		func() error { return r.captureStats("after-maintenance") },
		func() error { return r.smokePair("post-soak") },
		func() error { return r.captureStats("after-post-smoke") },
		r.postflightAndCleanup,
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	tarPath, sum, err := r.packageArtifact("completed_pending_review")
	if err != nil {
		return err
	}
	r.packaged = true
	fmt.Fprintf(r.stdout, "soak packet complete\nartifact: %s\ntar: %s\nsha256: %s\n", r.opts.Artifact, tarPath, sum)
	return nil
}

func (r *soakRun) init() error {
	dirs := []string{"bin", "cleanup", "logs", "maintenance", "postflight", "preflight", "proof", "run/rank1", "safe", "smoke", "stats", "supervisor", "tcpdiag"}
	for _, dir := range dirs {
		if err := os.MkdirAll(filepath.Join(r.opts.Artifact, dir), 0777); err != nil {
			return fmt.Errorf("create artifact dir %s: %w", dir, err)
		}
	}
	_ = os.Chmod(filepath.Join(r.opts.Artifact, "run"), 0700)
	_ = os.Chmod(filepath.Join(r.opts.Artifact, "run", "rank1"), 0700)
	head, err := commandOutput(r.opts.Root, "git", "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	r.head = head
	r.remoteArt = filepath.Join(r.opts.RemoteTmp, filepath.Base(r.opts.Artifact))
	r.coordinator = fmt.Sprintf("%s:%d", r.opts.RemoteRDMAIP, r.opts.CoordinatorPort)
	readme := fmt.Sprintf(`# gojaccl %s soak

- status: started
- commit: %s
- artifact: %s
- remote_artifact: %s
- device: %s
- remote_device: %s
- local_rdma_ip: %s
- remote_rdma_ip: %s
- local_route_interface: %s
- remote_route_interface: %s
- coordinator: %s
- soak_seconds: %d
- soak_interval_seconds: %d
- soak_rounds: %d
- no_retry: one-shot; stop on first required gate failure
`, r.opts.Device, r.head, r.opts.Artifact, r.remoteArt, r.opts.Device, r.opts.RemoteDevice, r.opts.LocalRDMAIP, r.opts.RemoteRDMAIP, r.opts.LocalRouteIface, r.opts.RemoteRouteIface, r.coordinator, r.opts.SoakSeconds, r.opts.SoakInterval, r.soakRounds())
	if err := os.WriteFile(filepath.Join(r.opts.Artifact, "README.md"), []byte(readme), 0666); err != nil {
		return fmt.Errorf("write README: %w", err)
	}
	noRetry := fmt.Sprintf("NO_RETRY: one-shot soak local %s remote %s. No retry after safe-gate/hash/metadata/route/tcpdiag/provider/RTR/CQ/smoke/maintenance/postflight/cleanup failure.\n", r.opts.Device, r.opts.RemoteDevice)
	if err := os.WriteFile(filepath.Join(r.opts.Artifact, "proof", "no-retry.txt"), []byte(noRetry), 0666); err != nil {
		return fmt.Errorf("write no-retry: %w", err)
	}
	return nil
}

func (r *soakRun) soakRounds() int {
	return (r.opts.SoakSeconds + r.opts.SoakInterval - 1) / r.opts.SoakInterval
}

func (r *soakRun) runSafeGates() error {
	dir := filepath.Join(r.opts.Artifact, "safe")
	r.capture(dir, "git-status", 0, "git", "status", "--short")
	r.capture(dir, "git-head", 0, "git", "rev-parse", "HEAD")
	r.capture(dir, "go-test", r.opts.CommandTimeout, "env", "-u", "CONFIRM_RDMA_EN1_SOAK_ONE_SHOT", "go", "test", "-count=1", "./...")
	r.capture(dir, "cgo0-go-test", r.opts.CommandTimeout, "env", "-u", "CONFIRM_RDMA_EN1_SOAK_ONE_SHOT", "CGO_ENABLED=0", "go", "test", "-count=1", "./...")
	r.capture(dir, "cgo0-go-vet", r.opts.CommandTimeout, "env", "-u", "CONFIRM_RDMA_EN1_SOAK_ONE_SHOT", "CGO_ENABLED=0", "go", "vet", "./...")
	r.capture(dir, "cgo0-go-race", r.opts.CommandTimeout, "env", "-u", "CONFIRM_RDMA_EN1_SOAK_ONE_SHOT", "CGO_ENABLED=0", "go", "test", "-race", "-count=1", "./...")
	r.capture(dir, "diff-check", 0, "git", "diff", "--check")
	r.capture(dir, "diff-integration", 0, "git", "diff", "--", "integration_test.go")
	r.capture(dir, "diff-allow-rtr", 0, "git", "diff", "-GALLOW_RTR|JACCL_TEST_RDMA_ALLOW_RTR", "--", ".")
	for _, name := range []string{"git-status", "git-head", "go-test", "cgo0-go-test", "cgo0-go-vet", "cgo0-go-race", "diff-check", "diff-integration", "diff-allow-rtr"} {
		if err := r.requireZero(name, filepath.Join(dir, name+".status")); err != nil {
			return err
		}
	}
	for _, name := range []string{"git-status", "diff-integration", "diff-allow-rtr"} {
		if err := r.requireEmpty(name, filepath.Join(dir, name+".out")); err != nil {
			return err
		}
	}
	return nil
}

func (r *soakRun) buildAndCopy() error {
	dir := filepath.Join(r.opts.Artifact, "proof")
	bin := filepath.Join(r.opts.Artifact, "bin")
	r.capture(dir, "build-gojaccl-test", r.opts.CommandTimeout, "go", "test", "-c", "-o", filepath.Join(bin, "gojaccl.test"), ".")
	r.capture(dir, "build-jaccld", r.opts.CommandTimeout, "go", "build", "-o", filepath.Join(bin, "jaccld"), "./cmd/jaccld")
	r.capture(dir, "build-jacclctl", r.opts.CommandTimeout, "go", "build", "-o", filepath.Join(bin, "jacclctl"), "./cmd/jacclctl")
	r.capture(dir, "build-jacclproof", r.opts.CommandTimeout, "go", "build", "-o", filepath.Join(bin, "jacclproof"), "./cmd/jacclproof")
	r.capture(dir, "local-binary-sha", 0, "shasum", "-a", "256", filepath.Join(bin, "gojaccl.test"), filepath.Join(bin, "jaccld"), filepath.Join(bin, "jacclctl"), filepath.Join(bin, "jacclproof"))
	remoteMkdir := fmt.Sprintf("mkdir -p %s %s %s %s %s %s %s %s %s %s && chmod 700 %s %s",
		shellQuote(filepath.Join(r.remoteArt, "bin")),
		shellQuote(filepath.Join(r.remoteArt, "cleanup")),
		shellQuote(filepath.Join(r.remoteArt, "logs")),
		shellQuote(filepath.Join(r.remoteArt, "maintenance")),
		shellQuote(filepath.Join(r.remoteArt, "postflight")),
		shellQuote(filepath.Join(r.remoteArt, "run", "rank0")),
		shellQuote(filepath.Join(r.remoteArt, "smoke")),
		shellQuote(filepath.Join(r.remoteArt, "supervisor")),
		shellQuote(filepath.Join(r.remoteArt, "tcpdiag")),
		shellQuote(filepath.Join(r.remoteArt, "stats")),
		shellQuote(filepath.Join(r.remoteArt, "run")),
		shellQuote(filepath.Join(r.remoteArt, "run", "rank0")),
	)
	r.remoteCapture(dir, "remote-mkdir", remoteMkdir)
	r.capture(dir, "scp-binaries", 0, append([]string{"scp", "-o", "BatchMode=yes", "-o", "ConnectTimeout=" + secondsString(r.opts.SSHTimeout)}, append([]string{
		filepath.Join(bin, "gojaccl.test"),
		filepath.Join(bin, "jaccld"),
		filepath.Join(bin, "jacclctl"),
		filepath.Join(bin, "jacclproof"),
	}, r.opts.Remote+":"+filepath.Join(r.remoteArt, "bin")+"/")...)...)
	r.remoteCapture(dir, "remote-binary-sha", "shasum -a 256 "+shellQuote(filepath.Join(r.remoteArt, "bin", "gojaccl.test"))+" "+shellQuote(filepath.Join(r.remoteArt, "bin", "jaccld"))+" "+shellQuote(filepath.Join(r.remoteArt, "bin", "jacclctl"))+" "+shellQuote(filepath.Join(r.remoteArt, "bin", "jacclproof")))
	for _, name := range []string{"build-gojaccl-test", "build-jaccld", "build-jacclctl", "build-jacclproof", "local-binary-sha", "remote-mkdir", "scp-binaries", "remote-binary-sha"} {
		if err := r.requireZero(name, filepath.Join(dir, name+".status")); err != nil {
			return err
		}
	}
	if firstFields(filepath.Join(dir, "local-binary-sha.out")) != firstFields(filepath.Join(dir, "remote-binary-sha.out")) {
		return r.stop("binary hashes differ")
	}
	return nil
}

func (r *soakRun) preflight() error {
	dir := filepath.Join(r.opts.Artifact, "preflight")
	r.capture(dir, "local-route", 0, "route", "-n", "get", r.opts.RemoteRDMAIP)
	r.remoteCapture(dir, "remote-route", "route -n get "+shellQuote(r.opts.LocalRDMAIP))
	if err := r.requireZero("local route", filepath.Join(dir, "local-route.status")); err != nil {
		return err
	}
	if err := r.requireZero("remote route", filepath.Join(dir, "remote-route.status")); err != nil {
		return err
	}
	if !fileContains(filepath.Join(dir, "local-route.out"), "interface: "+r.opts.LocalRouteIface) {
		return r.stop("local route not " + r.opts.LocalRouteIface)
	}
	if !fileContains(filepath.Join(dir, "remote-route.out"), "interface: "+r.opts.RemoteRouteIface) {
		return r.stop("remote route not " + r.opts.RemoteRouteIface)
	}
	metaArt := filepath.Join(dir, artifactDeviceName(r.opts.Device)+"-metadata")
	args := []string{
		filepath.Join(r.opts.Artifact, "bin", "jacclproof"),
		"rdma-metadata",
		"-device", r.opts.Device,
		"-remote-device", r.opts.RemoteDevice,
		"-remote", r.opts.Remote,
		"-remote-tmp", r.opts.RemoteTmp,
		"-art", metaArt,
		"-expected-selected-gid-index", strconv.Itoa(r.opts.ExpectedGIDIndex),
		"-timeout", "40s",
		"-ssh-connect-timeout", secondsString(r.opts.SSHTimeout) + "s",
	}
	r.capture(dir, "metadata-packet", 0, args...)
	if err := r.requireZero("metadata-packet", filepath.Join(dir, "metadata-packet.status")); err != nil {
		return err
	}
	r.capture(dir, "local-stale-processes", 0, filepath.Join(r.opts.Artifact, "bin", "jacclproof"), "process-snapshot")
	r.remoteCapture(dir, "remote-stale-processes", shellQuote(filepath.Join(r.remoteArt, "bin", "jacclproof"))+" process-snapshot")
	if err := r.requireProcessEmpty("local-stale-processes", filepath.Join(dir, "local-stale-processes")); err != nil {
		return err
	}
	return r.requireProcessEmpty("remote-stale-processes", filepath.Join(dir, "remote-stale-processes"))
}

func (r *soakRun) tcpdiag() error {
	dir := filepath.Join(r.opts.Artifact, "tcpdiag")
	port1 := r.opts.CoordinatorPort + 1
	port2 := r.opts.CoordinatorPort + 2
	remoteListen := shellQuote(filepath.Join(r.remoteArt, "bin", "jacclctl")) + " tcp-diagnostic -listen " + shellQuote(fmt.Sprintf("%s:%d", r.opts.RemoteRDMAIP, port1)) + " -timeout 30s"
	proc, err := r.startRemoteCapture(dir, "remote-listen-local-dial.listen", remoteListen)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	r.capture(dir, "remote-listen-local-dial.dial", 0, filepath.Join(r.opts.Artifact, "bin", "jacclctl"), "tcp-diagnostic", "-dial", fmt.Sprintf("%s:%d", r.opts.RemoteRDMAIP, port1), "-timeout", "30s")
	r.waitCapture(proc, filepath.Join(dir, "remote-listen-local-dial.listen.status"))
	if err := r.requireZero("tcpdiag remote listen", filepath.Join(dir, "remote-listen-local-dial.listen.status")); err != nil {
		return err
	}
	if err := r.requireZero("tcpdiag local dial", filepath.Join(dir, "remote-listen-local-dial.dial.status")); err != nil {
		return err
	}

	localListen := exec.Command(filepath.Join(r.opts.Artifact, "bin", "jacclctl"), "tcp-diagnostic", "-listen", fmt.Sprintf("%s:%d", r.opts.LocalRDMAIP, port2), "-timeout", "30s")
	proc, err = r.startLocalCapture(dir, "local-listen-remote-dial.listen", localListen)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	r.remoteCapture(dir, "local-listen-remote-dial.dial", shellQuote(filepath.Join(r.remoteArt, "bin", "jacclctl"))+" tcp-diagnostic -dial "+shellQuote(fmt.Sprintf("%s:%d", r.opts.LocalRDMAIP, port2))+" -timeout 30s")
	r.waitCapture(proc, filepath.Join(dir, "local-listen-remote-dial.listen.status"))
	if err := r.requireZero("tcpdiag local listen", filepath.Join(dir, "local-listen-remote-dial.listen.status")); err != nil {
		return err
	}
	return r.requireZero("tcpdiag remote dial", filepath.Join(dir, "local-listen-remote-dial.dial.status"))
}

func (r *soakRun) writeWrappers() error {
	dir := filepath.Join(r.opts.Artifact, "supervisor")
	rank1 := fmt.Sprintf(`#!/usr/bin/env bash
set -u
ART=%s
JACCLD="$ART/bin/jaccld"
SOCKET="$ART/run/rank1/jaccld.sock"
LOG="$ART/logs/jaccld-rank1.log"
PIDFILE="$ART/logs/jaccld-rank1.pid"
EXITFILE="$ART/logs/jaccld-rank1.exit"
mkdir -p "$(dirname "$SOCKET")" "$(dirname "$LOG")"
rm -f "$SOCKET" "$EXITFILE"
"$JACCLD" -socket "$SOCKET" -device %s -rank 1 -size 2 -coordinator %s -allow-remote-tcpchan >"$LOG" 2>&1 &
pid=$!
echo "$pid" >"$PIDFILE"
trap 'kill "$pid" 2>/dev/null || true' INT TERM
wait "$pid"
st=$?
echo "$st" >"$EXITFILE"
exit "$st"
`, shellQuote(r.opts.Artifact), shellQuote(r.opts.Device), shellQuote(r.coordinator))
	rank0 := fmt.Sprintf(`#!/usr/bin/env bash
set -u
ART=%s
JACCLD="$ART/bin/jaccld"
SOCKET="$ART/run/rank0/jaccld.sock"
LOG="$ART/logs/jaccld-rank0.log"
PIDFILE="$ART/logs/jaccld-rank0.pid"
EXITFILE="$ART/logs/jaccld-rank0.exit"
mkdir -p "$(dirname "$SOCKET")" "$(dirname "$LOG")"
rm -f "$SOCKET" "$EXITFILE"
"$JACCLD" -socket "$SOCKET" -device %s -rank 0 -size 2 -coordinator %s -allow-remote-tcpchan >"$LOG" 2>&1 &
pid=$!
echo "$pid" >"$PIDFILE"
trap 'kill "$pid" 2>/dev/null || true' INT TERM
wait "$pid"
st=$?
echo "$st" >"$EXITFILE"
exit "$st"
`, shellQuote(r.remoteArt), shellQuote(r.opts.RemoteDevice), shellQuote(r.coordinator))
	if err := os.WriteFile(filepath.Join(dir, "rank1-wrapper.sh"), []byte(rank1), 0777); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "rank0-wrapper.sh"), []byte(rank0), 0777); err != nil {
		return err
	}
	r.capture(dir, "scp-rank0-wrapper", 0, "scp", "-o", "BatchMode=yes", "-o", "ConnectTimeout="+secondsString(r.opts.SSHTimeout), filepath.Join(dir, "rank0-wrapper.sh"), r.opts.Remote+":"+filepath.Join(r.remoteArt, "supervisor", "rank0-wrapper.sh"))
	r.remoteCapture(dir, "remote-wrapper-chmod", "chmod +x "+shellQuote(filepath.Join(r.remoteArt, "supervisor", "rank0-wrapper.sh")))
	if err := r.requireZero("scp rank0 wrapper", filepath.Join(dir, "scp-rank0-wrapper.status")); err != nil {
		return err
	}
	return r.requireZero("remote wrapper chmod", filepath.Join(dir, "remote-wrapper-chmod.status"))
}

func (r *soakRun) launchDaemons() error {
	dir := filepath.Join(r.opts.Artifact, "supervisor")
	session := "gojaccl-soak-" + r.opts.Stamp
	_ = os.WriteFile(filepath.Join(dir, "local-tmux-session.txt"), []byte(session+"-rank1\n"), 0666)
	_ = os.WriteFile(filepath.Join(dir, "remote-screen-session.txt"), []byte(session+"-rank0\n"), 0666)
	r.capture(dir, "local-tmux-path", 0, "sh", "-c", "command -v tmux")
	r.remoteCapture(dir, "remote-screen-path", "command -v screen")
	if err := r.requireZero("local tmux path", filepath.Join(dir, "local-tmux-path.status")); err != nil {
		return err
	}
	if err := r.requireZero("remote screen path", filepath.Join(dir, "remote-screen-path.status")); err != nil {
		return err
	}
	r.remoteCapture(dir, "remote-screen-launch", "screen -dmS "+shellQuote(session+"-rank0")+" bash "+shellQuote(filepath.Join(r.remoteArt, "supervisor", "rank0-wrapper.sh")))
	time.Sleep(time.Second)
	r.capture(dir, "local-tmux-launch", 0, "tmux", "new-session", "-d", "-s", session+"-rank1", "bash", filepath.Join(dir, "rank1-wrapper.sh"))
	if err := r.requireZero("remote screen launch", filepath.Join(dir, "remote-screen-launch.status")); err != nil {
		return err
	}
	if err := r.requireZero("local tmux launch", filepath.Join(dir, "local-tmux-launch.status")); err != nil {
		return err
	}
	r.started = true
	return nil
}

func (r *soakRun) postIPCLivenessGate() error {
	deadline := time.Now().Add(120 * time.Second)
	ok := false
	for time.Now().Before(deadline) {
		r.fetchRemoteLog()
		if fileContains(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank0.log"), "phase=ipc_listen start") &&
			fileContains(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank1.log"), "phase=ipc_listen start") {
			ok = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	marker := "0\n"
	if ok {
		marker = "1\n"
	}
	_ = os.WriteFile(filepath.Join(r.opts.Artifact, "supervisor", "ipc-listen-wait-ok.marker"), []byte(marker), 0666)
	if !ok {
		if reason := earlyIPCStopReason(r.opts.Artifact); reason != "" {
			return r.stop(reason)
		}
		return r.stop("ipc_listen wait failed")
	}
	pid := strings.TrimSpace(readFile(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank1.pid")))
	r.capture(filepath.Join(r.opts.Artifact, "supervisor"), "local-rank1-ps", 0, "ps", "-p", pid, "-o", "pid=", "-o", "stat=", "-o", "command=")
	if _, err := os.Stat(filepath.Join(r.opts.Artifact, "run", "rank1", "jaccld.sock")); err == nil {
		_ = os.WriteFile(filepath.Join(r.opts.Artifact, "supervisor", "local-rank1-socket.status"), []byte("0\n"), 0666)
	} else {
		_ = os.WriteFile(filepath.Join(r.opts.Artifact, "supervisor", "local-rank1-socket.status"), []byte("1\n"), 0666)
	}
	r.remoteCapture(filepath.Join(r.opts.Artifact, "supervisor"), "remote-rank0-ps", "test -s "+shellQuote(filepath.Join(r.remoteArt, "logs", "jaccld-rank0.pid"))+" && ps -p $(cat "+shellQuote(filepath.Join(r.remoteArt, "logs", "jaccld-rank0.pid"))+") -o pid= -o stat= -o command=")
	r.remoteCapture(filepath.Join(r.opts.Artifact, "supervisor"), "remote-rank0-socket", "test -S "+shellQuote(filepath.Join(r.remoteArt, "run", "rank0", "jaccld.sock")))
	for _, name := range []string{"local-rank1-ps", "local-rank1-socket", "remote-rank0-ps", "remote-rank0-socket"} {
		if err := r.requireZero(name, filepath.Join(r.opts.Artifact, "supervisor", name+".status")); err != nil {
			return err
		}
	}
	r.writeLogAfterIPC("rank0")
	r.writeLogAfterIPC("rank1")
	if logHasStopMarker(filepath.Join(r.opts.Artifact, "supervisor", "remote-rank0-log-after-ipc.log")) ||
		logHasStopMarker(filepath.Join(r.opts.Artifact, "supervisor", "local-rank1-log-after-ipc.log")) {
		return r.stop("post-ipc log marker")
	}
	return r.captureStats("after-ipc")
}

func (r *soakRun) smokePair(label string) error {
	dir := filepath.Join(r.opts.Artifact, "smoke")
	remoteEnv := []string{
		"JACCL_BACKEND=daemon",
		"JACCL_DAEMON_SOCKET=" + filepath.Join(r.remoteArt, "run", "rank0", "jaccld.sock"),
		"JACCL_TEST_RDMA_CHILD=1",
		"JACCL_TEST_RDMA_ALLOW_RTR=1",
		"JACCL_TEST_RDMA=1",
		"JACCL_TEST_RANK=0",
		"JACCL_TEST_SIZE=2",
		"JACCL_TEST_COORDINATOR=" + r.coordinator,
		"JACCL_TEST_RDMA_DEVICE=" + r.opts.RemoteDevice,
		"JACCL_TEST_OP=barrier-sum",
	}
	remoteCmd := "cd " + shellQuote(filepath.Join(r.remoteArt, "bin")) + " && env " + joinShellArgs(remoteEnv) + " perl -e 'alarm shift; exec @ARGV' " + strconv.Itoa(int(r.opts.CommandTimeout/time.Second)) + " ./gojaccl.test -test.run '^TestIntegrationChild$' -test.v"
	proc, err := r.startRemoteCapture(dir, label+"-rank0", remoteCmd)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	localEnv := []string{
		"JACCL_BACKEND=daemon",
		"JACCL_DAEMON_SOCKET=" + filepath.Join(r.opts.Artifact, "run", "rank1", "jaccld.sock"),
		"JACCL_TEST_RDMA_CHILD=1",
		"JACCL_TEST_RDMA_ALLOW_RTR=1",
		"JACCL_TEST_RDMA=1",
		"JACCL_TEST_RANK=1",
		"JACCL_TEST_SIZE=2",
		"JACCL_TEST_COORDINATOR=" + r.coordinator,
		"JACCL_TEST_RDMA_DEVICE=" + r.opts.Device,
		"JACCL_TEST_OP=barrier-sum",
	}
	args := append([]string{"env"}, localEnv...)
	args = append(args, "perl", "-e", "alarm shift; exec @ARGV", strconv.Itoa(int(r.opts.CommandTimeout/time.Second)), filepath.Join(r.opts.Artifact, "bin", "gojaccl.test"), "-test.run", "^TestIntegrationChild$", "-test.v")
	r.capture(dir, label+"-rank1", 0, args...)
	r.waitCapture(proc, filepath.Join(dir, label+"-rank0.status"))
	if err := r.requireZero(label+" rank0 smoke", filepath.Join(dir, label+"-rank0.status")); err != nil {
		return err
	}
	return r.requireZero(label+" rank1 smoke", filepath.Join(dir, label+"-rank1.status"))
}

func (r *soakRun) maintenanceWindow() error {
	dir := filepath.Join(r.opts.Artifact, "maintenance")
	r.fetchRemoteLog()
	base0 := countInFile(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank0.log"), "jaccld maintenance peer=1 ok=true")
	base1 := countInFile(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank1.log"), "jaccld maintenance peer=0 ok=true")
	base0Lines := lineCount(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank0.log"))
	base1Lines := lineCount(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank1.log"))
	start := time.Now()
	success0, success1 := 0, 0
	firstFailure := ""
	rounds := r.soakRounds()
	for round := 1; round <= rounds; round++ {
		tag := fmt.Sprintf("round-%04d", round)
		_ = os.WriteFile(filepath.Join(dir, tag+".marker"), []byte(fmt.Sprintf("round %04d start %s\n", round, time.Now().UTC().Format(time.RFC3339))), 0666)
		remoteCmd := shellQuote(filepath.Join(r.remoteArt, "bin", "jacclctl")) + " -socket " + shellQuote(filepath.Join(r.remoteArt, "run", "rank0", "jaccld.sock")) + " maintain -timeout " + shellQuote(r.opts.MaintainTimeout.String())
		proc, err := r.startRemoteCapture(dir, tag+"-rank0", remoteCmd)
		if err != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)
		r.capture(dir, tag+"-rank1", 0, filepath.Join(r.opts.Artifact, "bin", "jacclctl"), "-socket", filepath.Join(r.opts.Artifact, "run", "rank1", "jaccld.sock"), "maintain", "-timeout", r.opts.MaintainTimeout.String())
		r.waitCapture(proc, filepath.Join(dir, tag+"-rank0.status"))
		st0 := readStatus(filepath.Join(dir, tag+"-rank0.status"))
		st1 := readStatus(filepath.Join(dir, tag+"-rank1.status"))
		r.fetchRemoteLog()
		if st0 != "0" || st1 != "0" {
			firstFailure = fmt.Sprintf("round %d command status rank0=%s rank1=%s", round, st0, st1)
			break
		}
		count0 := countInFile(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank0.log"), "jaccld maintenance peer=1 ok=true")
		count1 := countInFile(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank1.log"), "jaccld maintenance peer=0 ok=true")
		if count0 < base0+round || count1 < base1+round {
			firstFailure = fmt.Sprintf("round %d missing ok=true counters rank0=%d rank1=%d", round, count0, count1)
			break
		}
		writeTailFrom(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank0.log"), base0Lines, filepath.Join(dir, "rank0-new-lines.log"))
		writeTailFrom(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank1.log"), base1Lines, filepath.Join(dir, "rank1-new-lines.log"))
		if maintenanceLogHasStopMarker(filepath.Join(dir, "rank0-new-lines.log")) || maintenanceLogHasStopMarker(filepath.Join(dir, "rank1-new-lines.log")) {
			firstFailure = fmt.Sprintf("round %d error marker in new daemon log lines", round)
			break
		}
		success0, success1 = round, round
		target := start.Add(time.Duration(round*r.opts.SoakInterval) * time.Second)
		if d := time.Until(target); d > 0 {
			time.Sleep(d)
		}
	}
	end := time.Now()
	summary := fmt.Sprintf("planned_rounds=%d\ninterval_seconds=%d\nsuccessful_rounds_rank0=%d\nsuccessful_rounds_rank1=%d\nfailed_rounds_rank0=%d\nfailed_rounds_rank1=%d\nstart_epoch=%d\nend_epoch=%d\nelapsed_seconds=%d\nfirst_failure=%s\n",
		rounds, r.opts.SoakInterval, success0, success1, rounds-success0, rounds-success1, start.Unix(), end.Unix(), int(end.Sub(start)/time.Second), firstFailure)
	_ = os.WriteFile(filepath.Join(dir, "summary.env"), []byte(summary), 0666)
	if firstFailure != "" {
		return r.stop(firstFailure)
	}
	return nil
}

func (r *soakRun) postflightAndCleanup() error {
	dir := filepath.Join(r.opts.Artifact, "postflight")
	r.capture(dir, "local-rdma", 40*time.Second, "rdma_ctl", "status")
	r.capture(dir, "local-ibv", 40*time.Second, "ibv_devinfo", "-d", r.opts.Device)
	r.remoteCapture(dir, "remote-rdma", "perl -e 'alarm shift; exec @ARGV' 40 rdma_ctl status")
	r.remoteCapture(dir, "remote-ibv", "perl -e 'alarm shift; exec @ARGV' 40 ibv_devinfo -d "+shellQuote(r.opts.RemoteDevice))
	for _, name := range []string{"local-rdma", "local-ibv", "remote-rdma", "remote-ibv"} {
		if err := r.requireZero(name, filepath.Join(dir, name+".status")); err != nil {
			return err
		}
	}
	if !fileContains(filepath.Join(dir, "local-rdma.out"), "enabled") {
		return r.stop("local rdma not enabled")
	}
	if !fileContains(filepath.Join(dir, "remote-rdma.out"), "enabled") {
		return r.stop("remote rdma not enabled")
	}
	if !fileContains(filepath.Join(dir, "local-ibv.out"), "PORT_ACTIVE") {
		return r.stop("local ibv not active")
	}
	if !fileContains(filepath.Join(dir, "remote-ibv.out"), "PORT_ACTIVE") {
		return r.stop("remote ibv not active")
	}
	r.cleanupDaemons()
	r.started = false
	time.Sleep(3 * time.Second)
	r.fetchRemoteLog()
	cleanup := filepath.Join(r.opts.Artifact, "cleanup")
	r.capture(cleanup, "local-rank1-exit", 0, "cat", filepath.Join(r.opts.Artifact, "logs", "jaccld-rank1.exit"))
	r.remoteCapture(cleanup, "remote-rank0-exit", "cat "+shellQuote(filepath.Join(r.remoteArt, "logs", "jaccld-rank0.exit")))
	r.capture(cleanup, "local-processes", 0, filepath.Join(r.opts.Artifact, "bin", "jacclproof"), "process-snapshot")
	r.remoteCapture(cleanup, "remote-processes", shellQuote(filepath.Join(r.remoteArt, "bin", "jacclproof"))+" process-snapshot")
	if err := r.requireProcessEmpty("local cleanup processes", filepath.Join(cleanup, "local-processes")); err != nil {
		return err
	}
	return r.requireProcessEmpty("remote cleanup processes", filepath.Join(cleanup, "remote-processes"))
}

func (r *soakRun) captureStats(label string) error {
	dir := filepath.Join(r.opts.Artifact, "stats")
	r.capture(dir, label+"-rank1", 0, filepath.Join(r.opts.Artifact, "bin", "jacclctl"), "-socket", filepath.Join(r.opts.Artifact, "run", "rank1", "jaccld.sock"), "stats", "-json")
	r.remoteCapture(dir, label+"-rank0", shellQuote(filepath.Join(r.remoteArt, "bin", "jacclctl"))+" -socket "+shellQuote(filepath.Join(r.remoteArt, "run", "rank0", "jaccld.sock"))+" stats -json")
	if err := r.requireZero(label+" rank1 stats", filepath.Join(dir, label+"-rank1.status")); err != nil {
		return err
	}
	return r.requireZero(label+" rank0 stats", filepath.Join(dir, label+"-rank0.status"))
}

func (r *soakRun) capture(dir, name string, timeout time.Duration, args ...string) {
	logCommand(filepath.Join(r.opts.Artifact, "proof", "commands.log"), args...)
	ctx := contextOrBackground(timeout)
	cmd := exec.CommandContext(ctx.context, args[0], args[1:]...)
	cmd.Dir = r.opts.Root
	out, err := cmd.Output()
	status, errData := captureResult(ctx, err)
	writeCapture(dir, name, out, errData, status)
}

func (r *soakRun) remoteCapture(dir, name, remoteCommand string) {
	cmd := exec.Command("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout="+secondsString(r.opts.SSHTimeout), r.opts.Remote, "bash -lc "+shellQuote(remoteCommand))
	cmd.Dir = r.opts.Root
	logRemoteCommand(filepath.Join(r.opts.Artifact, "proof", "commands.log"), r.opts.Remote, remoteCommand)
	out, err := cmd.Output()
	status, errData := captureResult(commandContext{}, err)
	writeCapture(dir, name, out, errData, status)
}

type captureProc struct {
	cmd     *exec.Cmd
	outFile *os.File
	errFile *os.File
}

func (r *soakRun) startRemoteCapture(dir, name, remoteCommand string) (*captureProc, error) {
	cmd := exec.Command("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout="+secondsString(r.opts.SSHTimeout), r.opts.Remote, remoteCommand)
	return r.startLocalCapture(dir, name, cmd)
}

func (r *soakRun) startLocalCapture(dir, name string, cmd *exec.Cmd) (*captureProc, error) {
	logCommand(filepath.Join(r.opts.Artifact, "proof", "commands.log"), cmd.Args...)
	outFile, err := os.Create(filepath.Join(dir, name+".out"))
	if err != nil {
		return nil, err
	}
	errFile, err := os.Create(filepath.Join(dir, name+".err"))
	if err != nil {
		outFile.Close()
		return nil, err
	}
	cmd.Stdout = outFile
	cmd.Stderr = errFile
	cmd.Dir = r.opts.Root
	if err := cmd.Start(); err != nil {
		outFile.Close()
		errFile.Close()
		writeCapture(dir, name, nil, []byte(err.Error()+"\n"), 1)
		return nil, err
	}
	return &captureProc{cmd: cmd, outFile: outFile, errFile: errFile}, nil
}

func (r *soakRun) waitCapture(proc *captureProc, statusPath string) {
	err := proc.cmd.Wait()
	_ = proc.outFile.Close()
	_ = proc.errFile.Close()
	status := 0
	if err != nil {
		status = commandStatus(err)
	}
	_ = os.WriteFile(statusPath, []byte(strconv.Itoa(status)+"\n"), 0666)
}

func (r *soakRun) fetchRemoteLog() {
	cmd := exec.Command("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout="+secondsString(r.opts.SSHTimeout), r.opts.Remote, "cat "+shellQuote(filepath.Join(r.remoteArt, "logs", "jaccld-rank0.log"))+" 2>/dev/null || true")
	out, err := cmd.Output()
	_ = err
	_ = os.WriteFile(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank0.log"), out, 0666)
}

func (r *soakRun) cleanupDaemons() {
	if !r.started {
		return
	}
	if pid := strings.TrimSpace(readFile(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank1.pid"))); pid != "" {
		r.capture(filepath.Join(r.opts.Artifact, "cleanup"), "local-kill", 0, "kill", "-TERM", pid)
	}
	r.remoteCapture(filepath.Join(r.opts.Artifact, "cleanup"), "remote-kill", "test -s "+shellQuote(filepath.Join(r.remoteArt, "logs", "jaccld-rank0.pid"))+" && kill -TERM $(cat "+shellQuote(filepath.Join(r.remoteArt, "logs", "jaccld-rank0.pid"))+") || true")
}

func (r *soakRun) onExit(status int) {
	if r.opts.Artifact == "" {
		return
	}
	proofDir := filepath.Join(r.opts.Artifact, "proof")
	if _, err := os.Stat(proofDir); err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(proofDir, "script-exit.status"), []byte(strconv.Itoa(status)+"\n"), 0666)
	if status != 0 && strings.TrimSpace(readFile(filepath.Join(proofDir, "stop-reason.txt"))) == "" {
		_ = os.WriteFile(filepath.Join(proofDir, "stop-reason.txt"), []byte(fmt.Sprintf("script exit status=%d\n", status)), 0666)
	}
	if r.started {
		dir := filepath.Join(r.opts.Artifact, "postflight")
		r.capture(dir, "exit-local-rdma", 40*time.Second, "rdma_ctl", "status")
		r.capture(dir, "exit-local-ibv", 40*time.Second, "ibv_devinfo", "-d", r.opts.Device)
		r.remoteCapture(dir, "exit-remote-rdma", "perl -e 'alarm shift; exec @ARGV' 40 rdma_ctl status")
		r.remoteCapture(dir, "exit-remote-ibv", "perl -e 'alarm shift; exec @ARGV' 40 ibv_devinfo -d "+shellQuote(r.opts.RemoteDevice))
		r.cleanupDaemons()
		r.started = false
		time.Sleep(3 * time.Second)
		r.fetchRemoteLog()
		cleanup := filepath.Join(r.opts.Artifact, "cleanup")
		r.capture(cleanup, "exit-local-rank1-exit", 0, "cat", filepath.Join(r.opts.Artifact, "logs", "jaccld-rank1.exit"))
		r.remoteCapture(cleanup, "exit-remote-rank0-exit", "cat "+shellQuote(filepath.Join(r.remoteArt, "logs", "jaccld-rank0.exit")))
		r.capture(cleanup, "exit-local-processes", 0, filepath.Join(r.opts.Artifact, "bin", "jacclproof"), "process-snapshot")
		r.remoteCapture(cleanup, "exit-remote-processes", shellQuote(filepath.Join(r.remoteArt, "bin", "jacclproof"))+" process-snapshot")
	}
	if !r.packaged {
		_, _, _ = r.packageArtifact("stopped_pending_review")
	}
}

func (r *soakRun) packageArtifact(state string) (string, string, error) {
	if _, err := os.Stat(filepath.Join(r.opts.Artifact, "proof", "script-exit.status")); err != nil {
		_ = os.WriteFile(filepath.Join(r.opts.Artifact, "proof", "script-exit.status"), []byte("0\n"), 0666)
	}
	elapsed := readSummaryEnvInt(filepath.Join(r.opts.Artifact, "maintenance", "summary.env"), "elapsed_seconds")
	summary := struct {
		State                    string `json:"state"`
		Commit                   string `json:"commit"`
		Artifact                 string `json:"artifact"`
		RemoteArtifact           string `json:"remote_artifact"`
		Device                   string `json:"device"`
		RemoteDevice             string `json:"remote_device"`
		LocalIP                  string `json:"local_ip"`
		RemoteIP                 string `json:"remote_ip"`
		LocalRouteInterface      string `json:"local_route_interface"`
		RemoteRouteInterface     string `json:"remote_route_interface"`
		Coordinator              string `json:"coordinator"`
		SoakSeconds              int    `json:"soak_seconds"`
		SoakIntervalSeconds      int    `json:"soak_interval_seconds"`
		PlannedRounds            int    `json:"planned_rounds"`
		ElapsedSeconds           int    `json:"elapsed_seconds"`
		Rank0MaintenanceOKCounts int    `json:"rank0_maintenance_ok_count"`
		Rank1MaintenanceOKCounts int    `json:"rank1_maintenance_ok_count"`
	}{
		State:                    state,
		Commit:                   r.head,
		Artifact:                 r.opts.Artifact,
		RemoteArtifact:           r.remoteArt,
		Device:                   r.opts.Device,
		RemoteDevice:             r.opts.RemoteDevice,
		LocalIP:                  r.opts.LocalRDMAIP,
		RemoteIP:                 r.opts.RemoteRDMAIP,
		LocalRouteInterface:      r.opts.LocalRouteIface,
		RemoteRouteInterface:     r.opts.RemoteRouteIface,
		Coordinator:              r.coordinator,
		SoakSeconds:              r.opts.SoakSeconds,
		SoakIntervalSeconds:      r.opts.SoakInterval,
		PlannedRounds:            r.soakRounds(),
		ElapsedSeconds:           elapsed,
		Rank0MaintenanceOKCounts: countInFile(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank0.log"), "jaccld maintenance peer=1 ok=true"),
		Rank1MaintenanceOKCounts: countInFile(filepath.Join(r.opts.Artifact, "logs", "jaccld-rank1.log"), "jaccld maintenance peer=0 ok=true"),
	}
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return "", "", err
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(r.opts.Artifact, "proof", "final-summary.json"), data, 0666); err != nil {
		return "", "", err
	}
	if err := writeManifestSHA256(r.opts.Artifact, filepath.Join(r.opts.Artifact, "manifest.sha256")); err != nil {
		return "", "", err
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
		return "", "", err
	}
	return tarPath, sum, nil
}

func (r *soakRun) requireZero(label, path string) error {
	if status := readStatus(path); status != "0" {
		return r.stop(fmt.Sprintf("%s status=%s", label, status))
	}
	return nil
}

func (r *soakRun) requireEmpty(label, path string) error {
	if data, err := os.ReadFile(path); err == nil && len(data) != 0 {
		return r.stop(label + " produced disqualifying output")
	}
	return nil
}

func (r *soakRun) requireProcessEmpty(label, stem string) error {
	status := readStatus(stem + ".status")
	if status == "0" || fileSize(stem+".out") > 0 {
		return r.stop(label + " found stale processes")
	}
	if status != "1" {
		return r.stop(fmt.Sprintf("%s process snapshot status=%s", label, status))
	}
	return nil
}

func (r *soakRun) stop(reason string) error {
	_ = os.WriteFile(filepath.Join(r.opts.Artifact, "proof", "stop-reason.txt"), []byte(reason+"\n"), 0666)
	return exitError{code: 1, err: fmt.Errorf("%s", reason)}
}

func contextOrBackground(timeout time.Duration) commandContext {
	if timeout <= 0 {
		return commandContext{context: context.Background()}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return commandContext{context: ctx, cancel: cancel}
}

type commandContext struct {
	context context.Context
	cancel  func()
}

func captureResult(ctx commandContext, err error) (int, []byte) {
	if ctx.cancel != nil {
		defer ctx.cancel()
	}
	if err == nil {
		return 0, nil
	}
	status := commandStatus(err)
	var errData []byte
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		errData = ee.Stderr
	} else {
		errData = []byte(err.Error() + "\n")
	}
	if ctx.context != nil && ctx.context.Err() != nil {
		status = 124
		errData = append(errData, []byte(ctx.context.Err().Error()+"\n")...)
	}
	return status, errData
}

func getenvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err == nil {
		return d
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return time.Duration(n) * time.Second
}

func firstFields(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var fields []string
	scan := bufio.NewScanner(strings.NewReader(string(data)))
	for scan.Scan() {
		line := strings.Fields(scan.Text())
		if len(line) > 0 {
			fields = append(fields, line[0])
		}
	}
	return strings.Join(fields, " ")
}

func fileContains(path, text string) bool {
	data, err := os.ReadFile(path)
	return err == nil && strings.Contains(string(data), text)
}

func readFile(path string) string {
	data, _ := os.ReadFile(path)
	return string(data)
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func countInFile(path, text string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return strings.Count(string(data), text)
}

func lineCount(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	if len(data) == 0 {
		return 0
	}
	return strings.Count(string(data), "\n")
}

func writeTailFrom(src string, skip int, dst string) {
	data, err := os.ReadFile(src)
	if err != nil {
		return
	}
	lines := strings.Split(string(data), "\n")
	if skip > len(lines) {
		skip = len(lines)
	}
	_ = os.WriteFile(dst, []byte(strings.Join(lines[skip:], "\n")), 0666)
}

var (
	ipcLogStopRE         = regexp.MustCompile(`(?i)panic|fatal|provider error|rtr error|change queue pair to RTR|cq error|poison|maintenance error`)
	maintenanceLogStopRE = regexp.MustCompile(`(?i)ok=false|poison|unexpected completion|provider error|rtr error|cq error|maintenance .*err|daemon maintenance .*err|panic|fatal`)
)

func logHasStopMarker(path string) bool {
	return ipcLogStopRE.MatchString(readFile(path))
}

func earlyIPCStopReason(artifact string) string {
	for _, entry := range []struct {
		rank string
		path string
	}{
		{"rank0", filepath.Join(artifact, "logs", "jaccld-rank0.log")},
		{"rank1", filepath.Join(artifact, "logs", "jaccld-rank1.log")},
	} {
		line := firstLogStopMarkerLine(entry.path)
		if line != "" {
			return "daemon stopped before ipc_listen: " + entry.rank + ": " + line
		}
	}
	return ""
}

func firstLogStopMarkerLine(path string) string {
	data := readFile(path)
	scan := bufio.NewScanner(strings.NewReader(data))
	for scan.Scan() {
		line := scan.Text()
		if ipcLogStopRE.MatchString(line) {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

func maintenanceLogHasStopMarker(path string) bool {
	return maintenanceLogStopRE.MatchString(readFile(path))
}

func (r *soakRun) writeLogAfterIPC(rank string) {
	in := filepath.Join(r.opts.Artifact, "logs", "jaccld-"+rank+".log")
	name := "local-rank1-log-after-ipc.log"
	if rank == "rank0" {
		name = "remote-rank0-log-after-ipc.log"
	}
	data := readFile(in)
	idx := strings.Index(data, "phase=ipc_listen start")
	if idx < 0 {
		_ = os.WriteFile(filepath.Join(r.opts.Artifact, "supervisor", name), nil, 0666)
		return
	}
	_ = os.WriteFile(filepath.Join(r.opts.Artifact, "supervisor", name), []byte(data[idx:]), 0666)
}

func readSummaryEnvInt(path, key string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	prefix := key + "="
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, prefix) {
			n, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, prefix)))
			return n
		}
	}
	return 0
}

func writeManifestSHA256(root, outPath string) error {
	var files []string
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Base(path) == "manifest.sha256" {
			return nil
		}
		files = append(files, path)
		return nil
	}); err != nil {
		return err
	}
	var b strings.Builder
	for _, path := range files {
		sum, err := fileSHA256(path)
		if err != nil {
			return err
		}
		fmt.Fprintf(&b, "%s  %s\n", sum, path)
	}
	return os.WriteFile(outPath, []byte(b.String()), 0666)
}
