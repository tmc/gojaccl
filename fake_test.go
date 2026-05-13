package jaccl

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tmc/gojaccl/internal/reduce"
	"github.com/tmc/gojaccl/internal/topology"
)

type fakeNetwork struct {
	size int

	mu       sync.Mutex
	closed   int
	sends    map[[2]int]chan []byte
	barriers map[int]*fakeBarrier
	reduces  map[int]*fakeReduce
	gathers  map[int]*fakeGather
}

type fakeBarrier struct {
	count int
	done  chan struct{}
}

type fakeReduce struct {
	count  int
	op     reductionOp
	dt     reduce.DType
	values [][]byte
	result []byte
	err    error
	done   chan struct{}
}

type fakeGather struct {
	count  int
	values [][]byte
	result []byte
	done   chan struct{}
}

type fakeBackend struct {
	rank int
	size int
	net  *fakeNetwork
	ring bool

	barrierSeq int
	reduceSeq  int
	gatherSeq  int
}

func newFakeNetwork(size int) *fakeNetwork {
	return &fakeNetwork{
		size:     size,
		sends:    make(map[[2]int]chan []byte),
		barriers: make(map[int]*fakeBarrier),
		reduces:  make(map[int]*fakeReduce),
		gathers:  make(map[int]*fakeGather),
	}
}

func useFakeBackend(t *testing.T, net *fakeNetwork) {
	t.Helper()
	old := backendFactory
	backendFactory = func(ctx context.Context, cfg Config) (backend, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		topo, _ := topology.Choose(cfg.Devices, cfg.PreferRing)
		return &fakeBackend{rank: cfg.Rank, size: len(cfg.Devices), net: net, ring: topo == topology.Ring}, nil
	}
	t.Cleanup(func() { backendFactory = old })
}

func fakeConfig(rank, size int) Config {
	devices := make([][][]string, size)
	for i := range devices {
		devices[i] = make([][]string, size)
		for j := range devices[i] {
			devices[i][j] = []string{}
			if i != j {
				devices[i][j] = []string{fmt.Sprintf("rdma%d%d", i, j)}
			}
		}
	}
	return Config{Rank: rank, Coordinator: "127.0.0.1:1", Devices: devices}
}

func writeDevices(t *testing.T, devices [][][]string) string {
	t.Helper()
	data, err := json.Marshal(devices)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "devices.json")
	if err := os.WriteFile(path, data, 0o666); err != nil {
		t.Fatal(err)
	}
	return path
}

func newFakeGroup(rank, size int, net *fakeNetwork) *Group {
	return &Group{
		rank:   rank,
		size:   size,
		b:      &fakeBackend{rank: rank, size: size, net: net},
		op:     make(chan struct{}, 1),
		closed: make(chan struct{}),
	}
}

func (b *fakeBackend) barrier(ctx context.Context) error {
	seq := b.barrierSeq
	b.barrierSeq++

	b.net.mu.Lock()
	state := b.net.barriers[seq]
	if state == nil {
		state = &fakeBarrier{done: make(chan struct{})}
		b.net.barriers[seq] = state
	}
	state.count++
	if state.count == b.size {
		close(state.done)
	}
	done := state.done
	b.net.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *fakeBackend) send(ctx context.Context, dst int, data []byte) error {
	if b.ring && !b.neighbor(dst) {
		return fmt.Errorf("rank %d is not a ring neighbor", dst)
	}
	ch := b.net.sendChan(b.rank, dst)
	payload := append([]byte(nil), data...)
	select {
	case ch <- payload:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *fakeBackend) recv(ctx context.Context, src int, dst []byte) error {
	if b.ring && !b.neighbor(src) {
		return fmt.Errorf("rank %d is not a ring neighbor", src)
	}
	ch := b.net.sendChan(src, b.rank)
	select {
	case payload := <-ch:
		if len(dst) < len(payload) {
			return fmt.Errorf("receive buffer has %d bytes, want %d", len(dst), len(payload))
		}
		copy(dst, payload)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *fakeBackend) allReduce(ctx context.Context, op reductionOp, dt reduce.DType, dst, src []byte) error {
	seq := b.reduceSeq
	b.reduceSeq++
	srcCopy := append([]byte(nil), src...)

	b.net.mu.Lock()
	state := b.net.reduces[seq]
	if state == nil {
		state = &fakeReduce{
			op:     op,
			dt:     dt,
			values: make([][]byte, b.size),
			done:   make(chan struct{}),
		}
		b.net.reduces[seq] = state
	}
	state.values[b.rank] = srcCopy
	state.count++
	if state.count == b.size {
		state.result, state.err = computeReduce(op, dt, state.values)
		close(state.done)
	}
	done := state.done
	b.net.mu.Unlock()

	select {
	case <-done:
		if state.err != nil {
			return state.err
		}
		copy(dst, state.result)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *fakeBackend) allGather(ctx context.Context, elemSize int, dst, src []byte) error {
	seq := b.gatherSeq
	b.gatherSeq++
	srcCopy := append([]byte(nil), src...)

	b.net.mu.Lock()
	state := b.net.gathers[seq]
	if state == nil {
		state = &fakeGather{
			values: make([][]byte, b.size),
			done:   make(chan struct{}),
		}
		b.net.gathers[seq] = state
	}
	state.values[b.rank] = srcCopy
	state.count++
	if state.count == b.size {
		for _, v := range state.values {
			state.result = append(state.result, v...)
		}
		close(state.done)
	}
	done := state.done
	b.net.mu.Unlock()

	select {
	case <-done:
		copy(dst, state.result)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *fakeBackend) close() error {
	b.net.mu.Lock()
	b.net.closed++
	b.net.mu.Unlock()
	return nil
}

func (b *fakeBackend) neighbor(rank int) bool {
	left := (b.rank - 1 + b.size) % b.size
	right := (b.rank + 1) % b.size
	return rank == left || rank == right
}

func (n *fakeNetwork) sendChan(src, dst int) chan []byte {
	key := [2]int{src, dst}
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := n.sends[key]
	if ch == nil {
		ch = make(chan []byte)
		n.sends[key] = ch
	}
	return ch
}

func computeReduce(op reductionOp, dt reduce.DType, values [][]byte) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}
	result := append([]byte(nil), values[0]...)
	for _, v := range values[1:] {
		var err error
		switch op {
		case reductionSum:
			err = reduce.Sum(dt, result, v)
		case reductionMax:
			err = reduce.Max(dt, result, v)
		case reductionMin:
			err = reduce.Min(dt, result, v)
		default:
			err = fmt.Errorf("unknown reduction op %d", op)
		}
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
