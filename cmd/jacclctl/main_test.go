package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/gojaccl/internal/rdma"
)

func TestRunMaintainCommandValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "bad timeout",
			args: []string{"-timeout", "0"},
			want: "timeout 0s must be positive",
		},
		{
			name: "unexpected argument",
			args: []string{"extra"},
			want: "unexpected maintain arguments",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runMaintainCommand(context.Background(), filepath.Join(t.TempDir(), "missing.sock"), tt.args)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runMaintainCommand = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRunMaintainCommandTimesOut(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "jacclctl-maintain-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "jaccld.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
			return
		}
		close(accepted)
	}()

	err = runMaintainCommand(context.Background(), socket, []string{"-timeout", "10ms"})
	conn := <-accepted
	if conn != nil {
		defer conn.Close()
	}
	if err == nil {
		t.Fatal("runMaintainCommand returned nil, want timeout")
	}
	var netErr net.Error
	if !errors.Is(err, context.DeadlineExceeded) && (!errors.As(err, &netErr) || !netErr.Timeout()) {
		t.Fatalf("runMaintainCommand = %v, want timeout", err)
	}
}

func TestTCPDiagnosticLoopback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	errc := make(chan error, 1)
	go func() {
		var out bytes.Buffer
		errc <- serveTCPDiagnostic(ctx, ln, []byte(defaultTCPDiagnosticPayload), &out)
	}()

	var out bytes.Buffer
	if err := tcpDiagnosticDial(ctx, addr, []byte(defaultTCPDiagnosticPayload), &out); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "tcp diagnostic dial ok") {
		t.Fatalf("diagnostic output = %q, want ok", out.String())
	}
}

func TestRunTCPDiagnosticCommandValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing mode",
			want: "set exactly one of -listen or -dial",
		},
		{
			name: "both modes",
			args: []string{"-listen", "127.0.0.1:0", "-dial", "127.0.0.1:1"},
			want: "set exactly one of -listen or -dial",
		},
		{
			name: "bad timeout",
			args: []string{"-dial", "127.0.0.1:1", "-timeout", "0"},
			want: "timeout 0s must be positive",
		},
		{
			name: "unexpected argument",
			args: []string{"-dial", "127.0.0.1:1", "extra"},
			want: "unexpected tcp-diagnostic arguments",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := runTCPDiagnosticCommand(context.Background(), tt.args, &out)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runTCPDiagnosticCommand = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRunRDMAMetadataCommandValidation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing device",
			want: "device is required",
		},
		{
			name: "unexpected argument",
			args: []string{"-device", "rdma_en3", "extra"},
			want: "unexpected rdma-metadata arguments",
		},
		{
			name: "bad max gids",
			args: []string{"-device", "rdma_en3", "-max-gids", "0"},
			want: "max-gids 0 must be positive",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := runRDMAMetadataCommand(context.Background(), tt.args, &out)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runRDMAMetadataCommand = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestFormatRDMAPortInfo(t *testing.T) {
	info := rdma.PortInfo{
		Device:           "rdma_en3",
		PortNum:          1,
		LID:              0,
		GIDTableLength:   2,
		GIDScanLimit:     64,
		SelectedGIDIndex: 1,
		GIDs: []rdma.GIDEntry{
			{
				Index: 0,
				Zero:  true,
			},
			{
				Index:      1,
				GID:        [16]byte{10: 0xff, 11: 0xff, 12: 172, 13: 31, 14: 253, 15: 2},
				IPv4Mapped: true,
			},
		},
	}
	var out bytes.Buffer
	formatRDMAPortInfo(&out, info)
	got := out.String()
	for _, want := range []string{
		"rdma metadata device=rdma_en3 port=1 lid=0 gid_tbl_len=2 gid_scan_limit=64 selected_gid_index=1",
		"gid index=0 value=:: ipv4_mapped=false zero=true",
		"gid index=1 value=::ffff:172.31.253.2 ipv4_mapped=true zero=false ipv4=172.31.253.2",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("metadata output missing %q in:\n%s", want, got)
		}
	}
}

func TestRDMAMetadataCommandDoesNotAllocateResources(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	start := strings.Index(text, "func runRDMAMetadataCommand")
	if start < 0 {
		t.Fatal("runRDMAMetadataCommand not found")
	}
	end := strings.Index(text[start:], "\nfunc formatRDMAPortInfo")
	if end < 0 {
		t.Fatal("formatRDMAPortInfo not found")
	}
	body := text[start : start+end]
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
		if strings.Contains(body, forbidden) {
			t.Fatalf("rdma metadata command must not call %s", forbidden)
		}
	}
}
