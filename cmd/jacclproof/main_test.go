package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
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

func TestProofCommandsRefuseWithoutConfirmation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "en1 metadata",
			args: []string{"rdma-metadata", "-device", "rdma_en1"},
			want: []string{"refusing to run", "CONFIRM_RDMA_EN1_METADATA_ONE_SHOT=one-shot-metadata", "one-shot metadata"},
		},
		{
			name: "en3 metadata",
			args: []string{"rdma-metadata", "-device", "rdma_en3"},
			want: []string{"refusing to run", "CONFIRM_RDMA_EN3_METADATA_ONE_SHOT=one-shot-metadata", "one-shot metadata"},
		},
		{
			name: "en1 soak",
			args: []string{"rdma-soak", "-device", "rdma_en1"},
			want: []string{"refusing to run", "CONFIRM_RDMA_EN1_SOAK_ONE_SHOT=one-shot-soak", "same-data-QP maintenance"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := run(tt.args, &stdout, &stderr)
			if code := exitCode(err); code != 2 {
				t.Fatalf("exit code = %d err=%v, want 2", code, err)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			got := stderr.String()
			for _, want := range tt.want {
				if !strings.Contains(got, want) {
					t.Fatalf("stderr missing %q in:\n%s", want, got)
				}
			}
		})
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

func TestRDMAMetadataConfirmedStillRequiresPeer(t *testing.T) {
	t.Setenv("CONFIRM_RDMA_EN1_METADATA_ONE_SHOT", "one-shot-metadata")
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
