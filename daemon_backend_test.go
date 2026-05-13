package jaccl

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tmc/gojaccl/internal/allocator"
	"github.com/tmc/gojaccl/internal/ipc"
)

func TestDaemonBackendP2P(t *testing.T) {
	tr := &daemonTransport{}
	slab, socket, cleanup := startDaemonBackendServer(t, tr)
	defer cleanup()
	tr.slab = slab
	tr.recvData = []byte("from-peer")

	g, err := NewGroup(context.Background(), daemonConfig(socket))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	if err := g.Barrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := g.Send(context.Background(), 1, []byte("to-peer")); err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, len(tr.recvData))
	if err := g.Recv(context.Background(), 1, dst); err != nil {
		t.Fatal(err)
	}
	if string(dst) != string(tr.recvData) {
		t.Fatalf("recv = %q, want %q", dst, tr.recvData)
	}
	calls := tr.calls()
	if len(calls) != 3 || calls[0].op != "barrier" || calls[1].op != "send" || calls[2].op != "recv" {
		t.Fatalf("transport calls = %+v", calls)
	}
	if got := string(calls[1].data); got != "to-peer" {
		t.Fatalf("send slab bytes = %q, want to-peer", got)
	}
	st := slab.Stats()
	if st.Leases != 0 || st.Used != 0 {
		t.Fatalf("slab stats after operations = %+v, want no live leases", st)
	}
}

func TestDaemonBackendCollectivesUnsupported(t *testing.T) {
	tr := &daemonTransport{}
	_, socket, cleanup := startDaemonBackendServer(t, tr)
	defer cleanup()

	g, err := NewGroup(context.Background(), daemonConfig(socket))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	dst := []int64{0}
	src := []int64{1}
	if err := AllSum(context.Background(), g, dst, src); !errors.Is(err, ErrDaemonCollective) {
		t.Fatalf("AllSum daemon backend = %v, want ErrDaemonCollective", err)
	}
	gatherDst := make([]int64, 2)
	if err := AllGather(context.Background(), g, gatherDst, src); !errors.Is(err, ErrDaemonCollective) {
		t.Fatalf("AllGather daemon backend = %v, want ErrDaemonCollective", err)
	}
}

func daemonConfig(socket string) Config {
	cfg := fakeConfig(0, 2)
	cfg.Backend = BackendDaemon
	cfg.DaemonSocket = socket
	return cfg
}

func startDaemonBackendServer(t *testing.T, transport ipc.Transport) (*allocator.Slab, string, func()) {
	t.Helper()
	dir := t.TempDir()
	slab, err := allocator.NewSlab(dir, 4096)
	if err != nil {
		t.Fatal(err)
	}
	server, err := ipc.NewServer(slab, transport)
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join("/tmp", fmt.Sprintf("jaccld-backend-%d-%d.sock", os.Getpid(), time.Now().UnixNano()))
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- server.ListenAndServe(ctx, socket)
	}()
	waitUnixSocket(t, socket)
	return slab, socket, func() {
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

func waitUnixSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		conn, err := net.Dial("unix", path)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %s was not ready", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type daemonTransportCall struct {
	op     string
	peer   int
	offset int64
	length int64
	data   []byte
}

type daemonTransport struct {
	mu       sync.Mutex
	slab     *allocator.Slab
	recvData []byte
	log      []daemonTransportCall
}

func (t *daemonTransport) Barrier(context.Context) error {
	t.record(daemonTransportCall{op: "barrier"})
	return nil
}

func (t *daemonTransport) Send(_ context.Context, peer int, offset, length int64) error {
	data := append([]byte(nil), t.slab.Bytes()[offset:offset+length]...)
	t.record(daemonTransportCall{op: "send", peer: peer, offset: offset, length: length, data: data})
	return nil
}

func (t *daemonTransport) Recv(_ context.Context, peer int, offset, length int64) error {
	t.record(daemonTransportCall{op: "recv", peer: peer, offset: offset, length: length})
	if int64(len(t.recvData)) != length {
		return fmt.Errorf("recv data length %d, want %d", len(t.recvData), length)
	}
	copy(t.slab.Bytes()[offset:offset+length], t.recvData)
	return nil
}

func (t *daemonTransport) record(call daemonTransportCall) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.log = append(t.log, call)
}

func (t *daemonTransport) calls() []daemonTransportCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]daemonTransportCall(nil), t.log...)
}
