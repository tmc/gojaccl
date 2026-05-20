package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestProcessSnapshotCommand(t *testing.T) {
	dir := t.TempDir()
	fixture := filepath.Join(dir, "ps.txt")
	if err := os.WriteFile(fixture, []byte(strings.Join([]string{
		"123 bash             /bin/bash -lc go build ./examples/rdma/rdmaperf",
		"124 jaccld           /tmp/proof/bin/jaccld -socket /tmp/jaccld.sock",
		"",
	}, "\n")), 0666); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RDMA_PROCESS_SNAPSHOT_PS_FILE", fixture)
	var stdout, stderr bytes.Buffer
	if err := run([]string{"process-snapshot"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if got := stdout.String(); !strings.Contains(got, "jaccld") || strings.Contains(got, "go build") {
		t.Fatalf("stdout = %q", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestProcessSnapshotCommandNoMatchesExitsOne(t *testing.T) {
	dir := t.TempDir()
	fixture := filepath.Join(dir, "ps.txt")
	if err := os.WriteFile(fixture, []byte("123 bash /bin/bash\n"), 0666); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RDMA_PROCESS_SNAPSHOT_PS_FILE", fixture)
	var stdout, stderr bytes.Buffer
	err := run([]string{"process-snapshot"}, &stdout, &stderr)
	if code := exitCode(err); code != 1 {
		t.Fatalf("exit code = %d err=%v, want 1", code, err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q, want empty", stdout.String(), stderr.String())
	}
}

func TestRDMASoakRefusesWithoutConfirmation(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"rdma-soak", "-device", "rdma_en1"}, &stdout, &stderr)
	if code := exitCode(err); code != 2 {
		t.Fatalf("exit code = %d err=%v, want 2", code, err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	got := stderr.String()
	for _, want := range []string{
		"refusing to run",
		"CONFIRM_RDMA_EN1_SOAK_ONE_SHOT=one-shot-soak",
		"explicit device/interface combinations are exploratory",
		"same-data-QP maintenance",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("stderr missing %q in:\n%s", want, got)
		}
	}
}

func TestTopologyCommandReportsSparseLine(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "devices.json")
	if err := os.WriteFile(file, []byte(`[
		[[], ["left"], []],
		[["left"], [], ["right"]],
		[[], ["right"], []]
	]`), 0666); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if err := run([]string{"topology", "-file", file, "-prefer-ring"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	got := stdout.String()
	for _, want := range []string{
		`"topology": "line"`,
		`"ranks": 3`,
		`"directed_edges": 4`,
		`"empty_edges": 2`,
		`"total_wires": 4`,
		`"devices": [`,
		`"primary_devices": [`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("stdout missing %q in:\n%s", want, got)
		}
	}
}

func TestDevicesCommandWritesTwoRankDualCableMatrix(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{"devices", "-ranks", "2", "-devices", "rdma_en1,rdma_en3"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	var matrix [][][]string
	if err := json.Unmarshal(stdout.Bytes(), &matrix); err != nil {
		t.Fatal(err)
	}
	if len(matrix) != 2 || len(matrix[0][1]) != 2 || matrix[0][1][0] != "rdma_en1" || matrix[0][1][1] != "rdma_en3" {
		t.Fatalf("matrix = %#v, want two-rank dual-cable matrix", matrix)
	}
	if len(matrix[1][0]) != 2 || matrix[1][0][0] != "rdma_en1" || matrix[1][0][1] != "rdma_en3" {
		t.Fatalf("matrix = %#v, want symmetric dual-cable matrix", matrix)
	}
}

func TestDevicesCommandAcceptsDirectedEdges(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run([]string{
		"devices",
		"-ranks", "2",
		"-devices", "rdma_en1",
		"-edge", "0,1=rdma_en3",
		"-edge", "1,0=rdma_en2",
	}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
	var matrix [][][]string
	if err := json.Unmarshal(stdout.Bytes(), &matrix); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(matrix[0][1], ","); got != "rdma_en3" {
		t.Fatalf("matrix[0][1] = %q, want rdma_en3", got)
	}
	if got := strings.Join(matrix[1][0], ","); got != "rdma_en2" {
		t.Fatalf("matrix[1][0] = %q, want rdma_en2", got)
	}
}

func TestDevicesCommandRejectsBadShape(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"devices", "-shape", "star"}, &stdout, &stderr)
	if code := exitCode(err); code != 2 {
		t.Fatalf("exit code = %d err=%v, want 2", code, err)
	}
	if err == nil || !strings.Contains(err.Error(), `unknown shape "star"`) {
		t.Fatalf("error = %v, want bad shape", err)
	}
}

func TestTopologyCommandRejectsMissingFile(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"topology"}, &stdout, &stderr)
	if code := exitCode(err); code != 2 {
		t.Fatalf("exit code = %d err=%v, want 2", code, err)
	}
	if err == nil || !strings.Contains(err.Error(), "file is required") {
		t.Fatalf("error = %v, want file required", err)
	}
}

func TestMetadataOptionsPreserveDeviceDefaults(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantMaxGID  int
		wantTimeout time.Duration
		wantErr     string
	}{
		{
			name:        "en1 requires expected gid",
			args:        []string{"-device", "rdma_en1", "-remote", "peer", "-remote-tmp", "/tmp"},
			wantMaxGID:  1024,
			wantTimeout: 40 * time.Second,
			wantErr:     "expected-selected-gid-index is required",
		},
		{
			name:        "en3 does not require expected gid",
			args:        []string{"-device", "rdma_en3", "-remote", "peer", "-remote-tmp", "/tmp"},
			wantMaxGID:  64,
			wantTimeout: 20 * time.Second,
		},
		{
			name:        "en2 supports remote device override",
			args:        []string{"-device", "rdma_en2", "-remote-device", "rdma_en3", "-remote", "peer", "-remote-tmp", "/tmp"},
			wantMaxGID:  64,
			wantTimeout: 20 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, profile, err := parseMetadataOptions(tt.args, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if opts.MaxGIDs != tt.wantMaxGID {
				t.Fatalf("MaxGIDs = %d, want %d", opts.MaxGIDs, tt.wantMaxGID)
			}
			if tt.name == "en2 supports remote device override" && opts.RemoteDevice != "rdma_en3" {
				t.Fatalf("RemoteDevice = %q, want rdma_en3", opts.RemoteDevice)
			}
			if opts.Timeout != tt.wantTimeout {
				t.Fatalf("Timeout = %s, want %s", opts.Timeout, tt.wantTimeout)
			}
			err = validateMetadataOptions(opts, profile)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateMetadataOptions: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateMetadataOptions = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestRDMAMetadataRequiresPeer(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"rdma-metadata", "-device", "rdma_en1", "-expected-selected-gid-index", "1"}, &stdout, &stderr)
	if code := exitCode(err); code != 2 {
		t.Fatalf("exit code = %d err=%v, want 2", code, err)
	}
	if err == nil || !strings.Contains(err.Error(), "remote is required") {
		t.Fatalf("error = %v, want missing remote", err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q, want empty", stdout.String(), stderr.String())
	}
}

func TestRDMAAllocRequiresPeer(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"rdma-alloc", "-device", "rdma_en2"}, &stdout, &stderr)
	if code := exitCode(err); code != 2 {
		t.Fatalf("exit code = %d err=%v, want 2", code, err)
	}
	if err == nil || !strings.Contains(err.Error(), "remote is required") {
		t.Fatalf("error = %v, want missing remote", err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q, want empty", stdout.String(), stderr.String())
	}
}

func TestRDMAInitRequiresPeer(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run([]string{"rdma-init", "-device", "rdma_en2"}, &stdout, &stderr)
	if code := exitCode(err); code != 2 {
		t.Fatalf("exit code = %d err=%v, want 2", code, err)
	}
	if err == nil || !strings.Contains(err.Error(), "remote is required") {
		t.Fatalf("error = %v, want missing remote", err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q, want empty", stdout.String(), stderr.String())
	}
}

func TestRDMASoakConfirmedStillRequiresPeerAndIPs(t *testing.T) {
	t.Setenv("CONFIRM_RDMA_EN1_SOAK_ONE_SHOT", "one-shot-soak")
	var stdout, stderr bytes.Buffer
	err := run([]string{"rdma-soak", "-device", "rdma_en1"}, &stdout, &stderr)
	if code := exitCode(err); code != 2 {
		t.Fatalf("exit code = %d err=%v, want 2", code, err)
	}
	if err == nil || !strings.Contains(err.Error(), "remote is required") {
		t.Fatalf("error = %v, want missing remote", err)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q, want empty", stdout.String(), stderr.String())
	}
}

func TestRDMASoakOptionsRequireReviewedCadence(t *testing.T) {
	opts, err := parseSoakOptions([]string{
		"-remote", "peer",
		"-remote-tmp", "/tmp",
		"-local-rdma-ip", "192.0.2.1",
		"-remote-rdma-ip", "192.0.2.2",
		"-soak-seconds", "7199",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSoakOptions(opts); err == nil || !strings.Contains(err.Error(), "at least 7200") {
		t.Fatalf("validateSoakOptions = %v, want minimum duration error", err)
	}

	opts.SoakSeconds = 7200
	opts.SoakInterval = 30
	if err := validateSoakOptions(opts); err == nil || !strings.Contains(err.Error(), "must remain 60") {
		t.Fatalf("validateSoakOptions = %v, want reviewed interval error", err)
	}
}

func TestRDMASoakOptionsSupportAsymmetricExplicitDevices(t *testing.T) {
	opts, err := parseSoakOptions([]string{
		"-device", "rdma_en3",
		"-remote-device", "rdma_en1",
		"-remote", "peer",
		"-remote-tmp", "/tmp",
		"-local-rdma-ip", "10.0.0.2",
		"-remote-rdma-ip", "10.0.0.1",
		"-local-route-interface", "en0",
		"-remote-route-interface", "en0",
		"-expected-selected-gid-index", "0",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Device != "rdma_en3" || opts.RemoteDevice != "rdma_en1" {
		t.Fatalf("devices = %q/%q, want rdma_en3/rdma_en1", opts.Device, opts.RemoteDevice)
	}
	if opts.LocalRouteIface != "en0" || opts.RemoteRouteIface != "en0" {
		t.Fatalf("route interfaces = %q/%q, want en0/en0", opts.LocalRouteIface, opts.RemoteRouteIface)
	}
	if err := validateSoakOptions(opts); err != nil {
		t.Fatalf("validateSoakOptions = %v", err)
	}
}

func TestRDMASoakPostflightUsesRemoteDevice(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("soak.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	if !strings.Contains(text, `"exit-remote-ibv", "perl -e 'alarm shift; exec @ARGV' 40 ibv_devinfo -d "+shellQuote(r.opts.RemoteDevice)`) {
		t.Fatal("exit remote ibv postflight must use RemoteDevice")
	}
	if strings.Contains(text, `"exit-remote-ibv", "perl -e 'alarm shift; exec @ARGV' 40 ibv_devinfo -d "+shellQuote(r.opts.Device)`) {
		t.Fatal("exit remote ibv postflight must not use local Device")
	}
}

func TestRDMASoakEarlyIPCStopReasonIncludesRTRLine(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	if err := os.Mkdir(logDir, 0777); err != nil {
		t.Fatal(err)
	}
	log := strings.Join([]string{
		`2026/05/19 23:53:11 jaccld phase=rtr start peer=1`,
		`2026/05/19 23:53:12 peer 1: change queue pair to RTR: errno 60`,
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(logDir, "jaccld-rank0.log"), []byte(log), 0666); err != nil {
		t.Fatal(err)
	}
	reason := earlyIPCStopReason(dir)
	if !strings.Contains(reason, "daemon stopped before ipc_listen: rank0:") ||
		!strings.Contains(reason, "change queue pair to RTR: errno 60") {
		t.Fatalf("earlyIPCStopReason = %q", reason)
	}
}

func TestRDMASoakFinalSummaryIncludesRemoteScope(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"proof", "logs", "maintenance"} {
		if err := os.Mkdir(filepath.Join(dir, name), 0777); err != nil {
			t.Fatal(err)
		}
	}
	run := soakRun{
		opts: soakOptions{
			Artifact:         dir,
			Device:           "rdma_en2",
			RemoteDevice:     "rdma_en3",
			LocalRDMAIP:      "10.0.0.2",
			RemoteRDMAIP:     "10.0.0.3",
			LocalRouteIface:  "en2",
			RemoteRouteIface: "en3",
			SoakSeconds:      7200,
			SoakInterval:     60,
		},
		remoteArt:   "/tmp/remote-art",
		coordinator: "10.0.0.3:39311",
		head:        "abc123",
	}
	if _, _, err := run.packageArtifact("stopped_pending_review"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "proof", "final-summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary map[string]any
	if err := json.Unmarshal(data, &summary); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"device":                 "rdma_en2",
		"remote_device":          "rdma_en3",
		"local_route_interface":  "en2",
		"remote_route_interface": "en3",
	} {
		if got, _ := summary[key].(string); got != want {
			t.Fatalf("summary[%s] = %q, want %q", key, got, want)
		}
	}
}

func TestRDMAMetadataPacketDoesNotAllocateResources(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("metadata.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	for _, forbidden := range []string{
		"NewProtectionDomain",
		"NewCompletionQueue",
		"NewQueuePair",
		"RegisterMemory",
		"NewMemoryRegion",
		"ReadyToReceive",
		"ReadyToSend",
		"PostSend",
		"PostRecv",
		"PostWrite",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("rdma metadata packet command must not call %s", forbidden)
		}
	}
}

func TestMetadataEvaluateAcceptsAggregateRDMACtlForDeviceActive(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"local", "remote", "proof"} {
		if err := os.Mkdir(filepath.Join(dir, name), 0777); err != nil {
			t.Fatal(err)
		}
	}
	ctl := filepath.Join(dir, "jacclctl")
	proof := filepath.Join(dir, "jacclproof")
	for _, path := range []string{ctl, proof} {
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0777); err != nil {
			t.Fatal(err)
		}
	}

	writeStatus := func(subdir, name string, status int) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, subdir, name+".status"), []byte(strconv.Itoa(status)+"\n"), 0666); err != nil {
			t.Fatal(err)
		}
	}
	writeOut := func(subdir, name, text string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, subdir, name+".out"), []byte(text), 0666); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{
		"build-jacclctl",
		"build-jacclproof",
		"remote-mkdir",
		"copy-jacclctl",
		"copy-jacclproof",
		"remote-jacclctl-sha256",
		"remote-jacclproof-sha256",
	} {
		writeStatus("proof", name, 0)
	}
	for _, name := range []string{"jacclctl-sha256", "jacclproof-sha256"} {
		writeStatus("local", name, 0)
	}
	writeOut("local", "jacclctl-sha256", "abc  jacclctl\n")
	writeOut("proof", "remote-jacclctl-sha256", "abc  jacclctl\n")
	writeOut("local", "jacclproof-sha256", "def  jacclproof\n")
	writeOut("proof", "remote-jacclproof-sha256", "def  jacclproof\n")

	for _, side := range []struct {
		name   string
		device string
	}{
		{"local", "rdma_en2"},
		{"remote", "rdma_en3"},
	} {
		for _, name := range []string{
			"preflight-rdma",
			"preflight-ibv",
			"rdma-metadata-" + side.device,
			"postflight-rdma",
			"postflight-ibv",
		} {
			writeStatus(side.name, name, 0)
		}
		for _, name := range []string{"preflight-processes", "postflight-processes"} {
			writeStatus(side.name, name, 1)
			writeOut(side.name, name, "")
		}
		writeOut(side.name, "preflight-rdma", "enabled\n")
		writeOut(side.name, "postflight-rdma", "enabled\n")
		ibv := "hca_id:\t" + side.device + "\n\t\t\tstate:\t\t\tPORT_ACTIVE (4)\n"
		writeOut(side.name, "preflight-ibv", ibv)
		writeOut(side.name, "postflight-ibv", ibv)
		writeOut(side.name, "rdma-metadata-"+side.device, "rdma metadata device="+side.device+" gid_scan_limit=64\n")
	}

	r := metadataRun{
		opts: metadataOptions{
			Device:           "rdma_en2",
			RemoteDevice:     "rdma_en3",
			MaxGIDs:          64,
			ExpectedGIDIndex: -1,
			Artifact:         dir,
		},
		profile:  metadataProfile{rdmaStatusMode: "device-active"},
		ctlBin:   ctl,
		proofBin: proof,
	}
	if failures := r.evaluate(); len(failures) != 0 {
		t.Fatalf("evaluate failures = %v", failures)
	}
}

func TestMetadataSummariesUseSideSpecificDevices(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"local", "remote"} {
		if err := os.Mkdir(filepath.Join(dir, name), 0777); err != nil {
			t.Fatal(err)
		}
	}
	r := metadataRun{
		opts: metadataOptions{
			Device:       "rdma_en2",
			RemoteDevice: "rdma_en3",
			Artifact:     dir,
		},
	}
	fixtures := []struct {
		side   string
		device string
		gid    string
	}{
		{"local", "rdma_en2", "fe80::1"},
		{"remote", "rdma_en3", "fe80::2"},
	}
	for _, f := range fixtures {
		text := strings.Join([]string{
			"rdma metadata device=" + f.device + " port=1 lid=1 gid_tbl_len=1024 gid_scan_limit=64 selected_gid_index=0",
			"gid index=0 value=" + f.gid + " ipv4_mapped=false zero=false",
			"gid index=1 value=:: ipv4_mapped=false zero=true",
			"",
		}, "\n")
		path := filepath.Join(dir, f.side, "rdma-metadata-"+f.device+".out")
		if err := os.WriteFile(path, []byte(text), 0666); err != nil {
			t.Fatal(err)
		}
		r.summarizeMetadata(f.side)
		got, err := os.ReadFile(filepath.Join(dir, f.side, "gid-summary.txt"))
		if err != nil {
			t.Fatal(err)
		}
		summary := string(got)
		for _, want := range []string{
			"rdma metadata device=" + f.device,
			"nonzero_gid_count=1",
			"ipv4_mapped_gid_count=0",
			f.gid,
		} {
			if !strings.Contains(summary, want) {
				t.Fatalf("%s summary missing %q in:\n%s", f.side, want, summary)
			}
		}
	}
}

func TestInitEvaluateAcceptsAsymmetricDevices(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"local", "remote", "proof"} {
		if err := os.Mkdir(filepath.Join(dir, name), 0777); err != nil {
			t.Fatal(err)
		}
	}
	ctl := filepath.Join(dir, "jacclctl")
	proof := filepath.Join(dir, "jacclproof")
	for _, path := range []string{ctl, proof} {
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0777); err != nil {
			t.Fatal(err)
		}
	}

	writeStatus := func(subdir, name string, status int) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, subdir, name+".status"), []byte(strconv.Itoa(status)+"\n"), 0666); err != nil {
			t.Fatal(err)
		}
	}
	writeOut := func(subdir, name, text string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, subdir, name+".out"), []byte(text), 0666); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{
		"build-jacclctl",
		"build-jacclproof",
		"remote-mkdir",
		"copy-jacclctl",
		"copy-jacclproof",
		"remote-jacclctl-sha256",
		"remote-jacclproof-sha256",
	} {
		writeStatus("proof", name, 0)
	}
	for _, name := range []string{"jacclctl-sha256", "jacclproof-sha256"} {
		writeStatus("local", name, 0)
	}
	writeOut("local", "jacclctl-sha256", "abc  jacclctl\n")
	writeOut("proof", "remote-jacclctl-sha256", "abc  jacclctl\n")
	writeOut("local", "jacclproof-sha256", "def  jacclproof\n")
	writeOut("proof", "remote-jacclproof-sha256", "def  jacclproof\n")

	for _, side := range []struct {
		name   string
		device string
	}{
		{"local", "rdma_en2"},
		{"remote", "rdma_en3"},
	} {
		for _, name := range []string{
			"preflight-rdma",
			"preflight-ibv",
			"rdma-init-" + side.device,
			"postflight-rdma",
			"postflight-ibv",
		} {
			writeStatus(side.name, name, 0)
		}
		for _, name := range []string{"preflight-processes", "postflight-processes"} {
			writeStatus(side.name, name, 1)
			writeOut(side.name, name, "")
		}
		writeOut(side.name, "preflight-rdma", "enabled\n")
		writeOut(side.name, "postflight-rdma", "enabled\n")
		ibv := "hca_id:\t" + side.device + "\n\t\t\tstate:\t\t\tPORT_ACTIVE (4)\n"
		writeOut(side.name, "preflight-ibv", ibv)
		writeOut(side.name, "postflight-ibv", ibv)
		writeOut(side.name, "rdma-init-"+side.device, strings.Join([]string{
			"rdma resource command=rdma-init device=" + side.device + " cq_capacity=4 mr_bytes=4096 qpn=7 qpn_nonzero=true addr_nonzero=true lkey_nonzero=true rkey_nonzero=false init=true rtr=false work_requests=false",
			"resource protection_domain=allocated",
			"resource completion_queue=allocated",
			"resource queue_pair=allocated",
			"resource memory_region=allocated",
			"",
		}, "\n"))
	}

	r := allocRun{
		opts: allocOptions{
			Device:       "rdma_en2",
			RemoteDevice: "rdma_en3",
			Command:      "rdma-init",
			CQCapacity:   4,
			MRBytes:      4096,
			InitQP:       true,
			Artifact:     dir,
		},
		ctlBin:   ctl,
		proofBin: proof,
	}
	if failures := r.evaluate(); len(failures) != 0 {
		t.Fatalf("evaluate failures = %v", failures)
	}
}

func TestRDMAAllocPacketDoesNotTransitionOrPost(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("alloc.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	start := strings.Index(text, "func runRDMAAllocPacket")
	if start < 0 {
		t.Fatal("runRDMAAllocPacket not found")
	}
	end := strings.Index(text[start:], "\nfunc runRDMAInitPacket")
	if end < 0 {
		t.Fatal("runRDMAInitPacket not found")
	}
	body := text[start : start+end]
	for _, forbidden := range []string{
		"InitQueuePair",
		"ReadyToReceive",
		"ReadyToSend",
		"PostSend",
		"PostRecv",
		"PostWrite",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("rdma allocation packet command must not call %s", forbidden)
		}
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var x exitError
	if errors.As(err, &x) {
		return x.code
	}
	return 1
}
