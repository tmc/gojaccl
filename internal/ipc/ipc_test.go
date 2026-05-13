package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/gojaccl/internal/allocator"
)

func TestClientAllocMapFree(t *testing.T) {
	slab, client, _, cleanup := startTestServer(t, 4096)
	defer cleanup()
	defer client.Close()

	lease, err := client.Alloc(context.Background(), 8)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Offset != 0 || lease.Length != 8 {
		t.Fatalf("lease = %+v, want offset 0 length 8", lease)
	}
	mapping, err := client.Map(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.Close()
	off := int(lease.Offset + 3)
	mapping.Data[off] = 99
	if slab.Bytes()[off] != 99 {
		t.Fatalf("slab byte = %d, want 99", slab.Bytes()[off])
	}
	st, err := client.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Used != 8 || st.Leases != 1 {
		t.Fatalf("stats = %+v, want one 8-byte lease", st)
	}
	if err := client.Free(context.Background(), lease.ID); err != nil {
		t.Fatal(err)
	}
	st, err = client.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Used != 0 || st.Leases != 0 {
		t.Fatalf("stats after free = %+v, want empty slab", st)
	}
}

func TestServerReclaimsLeasesOnDisconnect(t *testing.T) {
	_, client, socket, cleanup := startTestServer(t, 64)
	defer cleanup()
	lease, err := client.Alloc(context.Background(), 16)
	if err != nil {
		t.Fatal(err)
	}
	if lease.ID == 0 {
		t.Fatal("lease ID = 0")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		next, err := Dial(context.Background(), socket)
		if err == nil {
			st, err := next.Stats(context.Background())
			next.Close()
			if err == nil && st.Leases == 0 && st.Used == 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("lease was not reclaimed after disconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestClientReportsServerErrors(t *testing.T) {
	_, client, _, cleanup := startTestServer(t, 8)
	defer cleanup()
	defer client.Close()
	if _, err := client.Alloc(context.Background(), 16); !errors.Is(err, allocator.ErrNoMemory) && !strings.Contains(err.Error(), allocator.ErrNoMemory.Error()) {
		t.Fatalf("Alloc too large = %v, want ErrNoMemory", err)
	}
	if err := client.Free(context.Background(), 99); !strings.Contains(err.Error(), allocator.ErrLeaseNotFound.Error()) {
		t.Fatalf("Free unknown = %v, want ErrLeaseNotFound", err)
	}
}

func TestDialMissingSocket(t *testing.T) {
	if _, err := Dial(context.Background(), filepath.Join(t.TempDir(), "missing.sock")); err == nil {
		t.Fatal("Dial missing socket = nil")
	}
}

func startTestServer(t *testing.T, size int64) (*allocator.Slab, *Client, string, func()) {
	t.Helper()
	dir := t.TempDir()
	slab, err := allocator.NewSlab(dir, size)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(slab)
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join("/tmp", fmt.Sprintf("jaccld-%d-%d.sock", os.Getpid(), time.Now().UnixNano()))
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- server.ListenAndServe(ctx, socket)
	}()
	waitSocket(t, socket)
	client, err := Dial(context.Background(), socket)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	return slab, client, socket, func() {
		client.Close()
		cancel()
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("server: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("server did not stop")
		}
		if err := slab.Close(); err != nil {
			t.Errorf("slab close: %v", err)
		}
		_ = os.Remove(socket)
	}
}

func waitSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			conn, err := net.Dial("unix", path)
			if err == nil {
				conn.Close()
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %s was not ready", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
