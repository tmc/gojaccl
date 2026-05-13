package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestClientSendRecvBarrier(t *testing.T) {
	tr := newFakeTransport()
	_, client, _, cleanup := startTestServerWithTransport(t, 64, tr)
	defer cleanup()
	defer client.Close()

	lease, err := client.Alloc(context.Background(), 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Send(context.Background(), 1, lease); err != nil {
		t.Fatal(err)
	}
	if err := client.Recv(context.Background(), 2, lease); err != nil {
		t.Fatal(err)
	}
	if err := client.Barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := tr.calls()
	want := []transportCall{
		{op: opSend, peer: 1, offset: lease.Offset, length: lease.Length},
		{op: opRecv, peer: 2, offset: lease.Offset, length: lease.Length},
		{op: opBarrier},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("transport calls = %+v, want %+v", got, want)
	}
}

func TestClientSendRecvErrors(t *testing.T) {
	tr := newFakeTransport()
	tr.err = errors.New("transport down")
	_, client, _, cleanup := startTestServerWithTransport(t, 64, tr)
	defer cleanup()
	defer client.Close()

	lease, err := client.Alloc(context.Background(), 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Send(context.Background(), 1, lease); err == nil || !strings.Contains(err.Error(), tr.err.Error()) {
		t.Fatalf("Send transport error = %v, want %v", err, tr.err)
	}
	if err := client.Recv(context.Background(), 1, allocator.Lease{ID: lease.ID, Offset: lease.Offset + 8, Length: 16}); err == nil || !strings.Contains(err.Error(), "outside lease") {
		t.Fatalf("Recv out of range = %v, want range error", err)
	}
	if err := client.Send(context.Background(), -1, lease); err == nil || !strings.Contains(err.Error(), "peer -1") {
		t.Fatalf("Send bad peer = %v, want peer error", err)
	}
}

func TestClientDataOpsNeedTransport(t *testing.T) {
	_, client, _, cleanup := startTestServer(t, 64)
	defer cleanup()
	defer client.Close()

	lease, err := client.Alloc(context.Background(), 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Send(context.Background(), 1, lease); err == nil || !strings.Contains(err.Error(), ErrNoTransport.Error()) {
		t.Fatalf("Send without transport = %v, want ErrNoTransport", err)
	}
	if err := client.Barrier(context.Background()); err == nil || !strings.Contains(err.Error(), ErrNoTransport.Error()) {
		t.Fatalf("Barrier without transport = %v, want ErrNoTransport", err)
	}
}

func TestCheckRange(t *testing.T) {
	lease := allocator.Lease{ID: 7, Offset: 10, Length: 20}
	leases := map[uint64]allocator.Lease{lease.ID: lease}
	tests := []struct {
		name string
		req  Request
	}{
		{
			name: "UnknownLease",
			req:  Request{LeaseID: 99, Peer: 1, Offset: 10, Length: 1},
		},
		{
			name: "NegativePeer",
			req:  Request{LeaseID: lease.ID, Peer: -1, Offset: 10, Length: 1},
		},
		{
			name: "NegativeLength",
			req:  Request{LeaseID: lease.ID, Peer: 1, Offset: 10, Length: -1},
		},
		{
			name: "BeforeLease",
			req:  Request{LeaseID: lease.ID, Peer: 1, Offset: 9, Length: 1},
		},
		{
			name: "AfterLease",
			req:  Request{LeaseID: lease.ID, Peer: 1, Offset: 29, Length: 2},
		},
		{
			name: "Overflow",
			req:  Request{LeaseID: lease.ID, Peer: 1, Offset: 1<<63 - 1, Length: 1},
		},
	}
	if err := checkRange(Request{LeaseID: lease.ID, Peer: 1, Offset: 12, Length: 8}, leases); err != nil {
		t.Fatalf("checkRange valid subrange: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkRange(tt.req, leases); err == nil {
				t.Fatal("checkRange = nil, want error")
			}
		})
	}
}

func TestDialMissingSocket(t *testing.T) {
	if _, err := Dial(context.Background(), filepath.Join(t.TempDir(), "missing.sock")); err == nil {
		t.Fatal("Dial missing socket = nil")
	}
}

func TestServerSocketOwnerOnly(t *testing.T) {
	_, client, socket, cleanup := startTestServer(t, 64)
	defer cleanup()
	defer client.Close()

	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0600 {
		t.Fatalf("socket mode = %#o, want 0600", mode)
	}
}

func startTestServer(t *testing.T, size int64) (*allocator.Slab, *Client, string, func()) {
	return startTestServerWithTransport(t, size, nil)
}

func startTestServerWithTransport(t *testing.T, size int64, transport Transport) (*allocator.Slab, *Client, string, func()) {
	t.Helper()
	dir := t.TempDir()
	slab, err := allocator.NewSlab(dir, size)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(slab, transport)
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

type transportCall struct {
	op     string
	peer   int
	offset int64
	length int64
}

type fakeTransport struct {
	mu  sync.Mutex
	err error
	log []transportCall
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{}
}

func (f *fakeTransport) Barrier(context.Context) error {
	f.record(transportCall{op: opBarrier})
	return f.err
}

func (f *fakeTransport) Send(_ context.Context, peer int, offset, length int64) error {
	f.record(transportCall{op: opSend, peer: peer, offset: offset, length: length})
	return f.err
}

func (f *fakeTransport) Recv(_ context.Context, peer int, offset, length int64) error {
	f.record(transportCall{op: opRecv, peer: peer, offset: offset, length: length})
	return f.err
}

func (f *fakeTransport) record(call transportCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, call)
}

func (f *fakeTransport) calls() []transportCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]transportCall(nil), f.log...)
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
