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

	"github.com/tmc/gojaccl/internal/jaccld/resource"
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
	dir, err := os.MkdirTemp("", "gjctl-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "s.sock")
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

func TestRunStatsCommandValidation(t *testing.T) {
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
			want: "unexpected stats arguments",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := runStatsCommand(context.Background(), filepath.Join(t.TempDir(), "missing.sock"), tt.args, &out)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runStatsCommand = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestFormatResourceStats(t *testing.T) {
	stats := resource.Stats{
		State:  resource.StateReady,
		Leases: 1,
		MemoryRegions: resource.PoolStats{
			InUse:          1,
			Available:      -1,
			BytesInUse:     16,
			BytesAvailable: 4080,
		},
		QueuePairs:       resource.PoolStats{InUse: 1, Available: 3},
		CompletionQueues: resource.PoolStats{InUse: 1, Available: 3},
		Slots: resource.SlotStats{
			BootID:             "boot-1",
			Source:             "jaccld-observed",
			StatePath:          "/tmp/jaccld/slots/boot-1.json",
			ExternalUseUnknown: true,
			ProtectionDomains:  resource.SlotCounter{Opened: 1, Outstanding: 1, Live: 1},
			MemoryRegions:      resource.SlotCounter{Opened: 1, Outstanding: 1, Live: 1},
			QueuePairs:         resource.SlotCounter{Opened: 2, Closed: 1, Outstanding: 1, Live: 1},
			CompletionQueues:   resource.SlotCounter{Failed: 1},
		},
	}
	var out bytes.Buffer
	formatResourceStats(&out, stats)
	got := out.String()
	for _, want := range []string{
		"jaccld resource state=ready leases=1",
		"pool memory_regions in_use=1 available=-1 bytes_in_use=16 bytes_available=4080",
		"slot_ledger boot_id=\"boot-1\" source=\"jaccld-observed\" external_use_unknown=true state_path=\"/tmp/jaccld/slots/boot-1.json\"",
		"slot kind=protection_domain opened=1 closed=0 outstanding=1 live=1 failed=0",
		"slot kind=queue_pair opened=2 closed=1 outstanding=1 live=1 failed=0",
		"slot kind=completion_queue opened=0 closed=0 outstanding=0 live=0 failed=1",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("stats output missing %q in:\n%s", want, got)
		}
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

func TestRunRDMAAllocCommandValidation(t *testing.T) {
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
			args: []string{"-device", "rdma_en2", "extra"},
			want: "unexpected rdma-alloc arguments",
		},
		{
			name: "bad cq capacity",
			args: []string{"-device", "rdma_en2", "-cq-capacity", "0"},
			want: "cq-capacity 0 must be positive",
		},
		{
			name: "bad mr bytes",
			args: []string{"-device", "rdma_en2", "-mr-bytes", "0"},
			want: "mr-bytes 0 must be positive",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := runRDMAAllocCommand(context.Background(), tt.args, &out)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runRDMAAllocCommand = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRunRDMAInitCommandValidation(t *testing.T) {
	var out bytes.Buffer
	err := runRDMAInitCommand(context.Background(), []string{"-device", "rdma_en2", "extra"}, &out)
	if err == nil || !strings.Contains(err.Error(), "unexpected rdma-init arguments") {
		t.Fatalf("runRDMAInitCommand = %v, want unexpected argument", err)
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

func TestFormatRDMAAllocation(t *testing.T) {
	var out bytes.Buffer
	formatRDMAResource(&out, "rdma-init", "rdma_en2", 4, 4096, true, nil, nil)
	got := out.String()
	for _, want := range []string{
		"rdma resource command=rdma-init device=rdma_en2 cq_capacity=4 mr_bytes=4096 qpn=0 qpn_nonzero=false addr_nonzero=false lkey_nonzero=false rkey_nonzero=false init=true rtr=false work_requests=false",
		"resource protection_domain=allocated",
		"resource completion_queue=allocated",
		"resource queue_pair=allocated",
		"resource memory_region=allocated",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("allocation output missing %q in:\n%s", want, got)
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

func TestRDMAAllocCommandDoesNotTransitionOrPost(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	start := strings.Index(text, "func runRDMAAllocCommand")
	if start < 0 {
		t.Fatal("runRDMAAllocCommand not found")
	}
	end := strings.Index(text[start:], "\nfunc runRDMAInitCommand")
	if end < 0 {
		t.Fatal("runRDMAInitCommand not found")
	}
	body := text[start : start+end]
	if strings.Contains(body, "InitQueuePair") {
		t.Fatal("rdma allocation command must not call InitQueuePair")
	}
}

func TestRDMAResourceCommandDoesNotRTROrPost(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(src)
	start := strings.Index(text, "func runRDMAResourceCommand")
	if start < 0 {
		t.Fatal("runRDMAResourceCommand not found")
	}
	end := strings.Index(text[start:], "\nfunc formatRDMAResource")
	if end < 0 {
		t.Fatal("formatRDMAResource not found")
	}
	body := text[start : start+end]
	for _, want := range []string{
		"NewProtectionDomain",
		"NewCompletionQueue",
		"NewQueuePair",
		"NewMemoryRegion",
		"InitQueuePair",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("rdma resource command missing %s", want)
		}
	}
	for _, forbidden := range []string{
		"RegisterMemory",
		"ReadyToReceive",
		"ReadyToSend",
		"PostSend",
		"PostRecv",
		"PostWrite",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("rdma allocation command must not call %s", forbidden)
		}
	}
}
