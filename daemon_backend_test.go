package jaccl

import (
	"context"
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
	tr.setSlab(slab)
	recvWant := []byte("from-peer")
	tr.setRecvData(recvWant)

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
	dst := make([]byte, len(recvWant))
	if err := g.Recv(context.Background(), 1, dst); err != nil {
		t.Fatal(err)
	}
	if string(dst) != string(recvWant) {
		t.Fatalf("recv = %q, want %q", dst, recvWant)
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

func TestDaemonBackendCollectives(t *testing.T) {
	reduceWant := []int64{3}
	gatherWant := []int64{1, 2}
	tr := &daemonTransport{
		reduceData: append([]byte(nil), sliceBytes(reduceWant)...),
		gatherData: append([]byte(nil), sliceBytes(gatherWant)...),
	}
	slab, socket, cleanup := startDaemonBackendServer(t, tr)
	tr.setSlab(slab)
	defer cleanup()

	g, err := NewGroup(context.Background(), daemonConfig(socket))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	dst := []int64{0}
	src := []int64{1}
	if err := AllSum(context.Background(), g, dst, src); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(dst) != fmt.Sprint(reduceWant) {
		t.Fatalf("AllSum dst = %v, want %v", dst, reduceWant)
	}
	gatherDst := make([]int64, 2)
	if err := AllGather(context.Background(), g, gatherDst, src); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(gatherDst) != fmt.Sprint(gatherWant) {
		t.Fatalf("AllGather dst = %v, want %v", gatherDst, gatherWant)
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
	socketDir, err := os.MkdirTemp("/tmp", "gojaccl-backend-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(socketDir)
	})
	if err := os.Chmod(socketDir, 0700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(socketDir, fmt.Sprintf("jaccld-backend-%d-%d.sock", os.Getpid(), time.Now().UnixNano()))
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
	mu         sync.Mutex
	slab       *allocator.Slab
	recvData   []byte
	reduceData []byte
	gatherData []byte
	log        []daemonTransportCall
}

func (t *daemonTransport) setSlab(slab *allocator.Slab) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.slab = slab
}

func (t *daemonTransport) setRecvData(data []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.recvData = append([]byte(nil), data...)
}

func (t *daemonTransport) slabBytes() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.slab.Bytes()
}

func (t *daemonTransport) recvBytes() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]byte(nil), t.recvData...)
}

func (t *daemonTransport) reduceBytes() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]byte(nil), t.reduceData...)
}

func (t *daemonTransport) gatherBytes() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]byte(nil), t.gatherData...)
}

func (t *daemonTransport) Barrier(context.Context) error {
	t.record(daemonTransportCall{op: "barrier"})
	return nil
}

func (t *daemonTransport) Send(_ context.Context, peer int, offset, length int64) error {
	data := append([]byte(nil), t.slabBytes()[offset:offset+length]...)
	t.record(daemonTransportCall{op: "send", peer: peer, offset: offset, length: length, data: data})
	return nil
}

func (t *daemonTransport) Recv(_ context.Context, peer int, offset, length int64) error {
	t.record(daemonTransportCall{op: "recv", peer: peer, offset: offset, length: length})
	data := t.recvBytes()
	if int64(len(data)) != length {
		return fmt.Errorf("recv data length %d, want %d", len(data), length)
	}
	copy(t.slabBytes()[offset:offset+length], data)
	return nil
}

func (t *daemonTransport) AllReduce(_ context.Context, op, dt int, dstOffset, dstLength, srcOffset, srcLength int64) error {
	t.record(daemonTransportCall{op: "allReduce", offset: srcOffset, length: srcLength})
	data := t.reduceBytes()
	if int64(len(data)) != dstLength {
		return fmt.Errorf("reduce data length %d, want %d", len(data), dstLength)
	}
	copy(t.slabBytes()[dstOffset:dstOffset+dstLength], data)
	return nil
}

func (t *daemonTransport) AllGather(_ context.Context, elemSize int, dstOffset, dstLength, srcOffset, srcLength int64) error {
	t.record(daemonTransportCall{op: "allGather", offset: srcOffset, length: srcLength})
	data := t.gatherBytes()
	if int64(len(data)) != dstLength {
		return fmt.Errorf("gather data length %d, want %d", len(data), dstLength)
	}
	copy(t.slabBytes()[dstOffset:dstOffset+dstLength], data)
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
