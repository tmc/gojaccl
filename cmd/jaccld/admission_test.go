package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/tmc/gojaccl/internal/reduce"
	"github.com/tmc/gojaccl/internal/tcpchan"
)

func TestAdmissionGateMaintenanceWaitsForInFlight(t *testing.T) {
	g := newAdmissionGate()
	release, err := g.enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		end, err := g.beginMaintenance(context.Background())
		if err == nil {
			end()
		}
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("beginMaintenance returned before in-flight release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionGateBlocksNewDataOpsDuringMaintenance(t *testing.T) {
	g := newAdmissionGate()
	end, err := g.beginMaintenance(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	entered := make(chan error, 1)
	go func() {
		release, err := g.enter(context.Background())
		if err == nil {
			release()
		}
		entered <- err
	}()

	select {
	case err := <-entered:
		t.Fatalf("enter returned while maintenance was active: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	end()
	if err := <-entered; err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionGateEnterHonorsContext(t *testing.T) {
	g := newAdmissionGate()
	end, err := g.beginMaintenance(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer end()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := g.enter(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("enter during maintenance = %v, want context deadline", err)
	}
}

func TestAdmissionGateMaintenanceCancelReopensAdmission(t *testing.T) {
	g := newAdmissionGate()
	release, err := g.enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := g.beginMaintenance(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("beginMaintenance with in-flight op = %v, want context deadline", err)
	}
	release()

	release, err = g.enter(context.Background())
	if err != nil {
		t.Fatalf("enter after canceled maintenance = %v", err)
	}
	release()
}

func TestDaemonTransportMaintenanceBlocksCollectives(t *testing.T) {
	slab := newTransportTestSlab(t)
	tp := &daemonTransport{
		rank:      0,
		size:      1,
		slab:      slab,
		admission: newAdmissionGate(),
	}
	src := allocBytes(t, slab, int64Bytes([]int64{5}))
	dst := allocBytes(t, slab, make([]byte, 8))

	end, err := tp.beginMaintenance(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err = tp.AllReduce(ctx, daemonReductionSum, int(reduce.Int64), dst.Offset, dst.Length, src.Offset, src.Length)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AllReduce during maintenance = %v, want context deadline", err)
	}
	end()

	if err := tp.AllReduce(context.Background(), daemonReductionSum, int(reduce.Int64), dst.Offset, dst.Length, src.Offset, src.Length); err != nil {
		t.Fatalf("AllReduce after maintenance = %v", err)
	}
}

func TestDaemonTransportMaintenanceWindowUsesSideBarriers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t0, t1 := newMaintenanceTransportPair(t, ctx)

	type result struct {
		finish func(context.Context) error
		err    error
	}
	start := func(t *daemonTransport) <-chan result {
		ch := make(chan result, 1)
		go func() {
			finish, err := t.beginMaintenanceWindow(ctx)
			ch <- result{finish: finish, err: err}
		}()
		return ch
	}

	r0c := start(t0)
	select {
	case r := <-r0c:
		t.Fatalf("rank 0 passed pre-barrier before rank 1: %v", r.err)
	case <-time.After(20 * time.Millisecond):
	}
	r1c := start(t1)
	r0 := <-r0c
	r1 := <-r1c
	if r0.err != nil {
		t.Fatal(r0.err)
	}
	if r1.err != nil {
		t.Fatal(r1.err)
	}

	done0 := make(chan error, 1)
	go func() { done0 <- r0.finish(ctx) }()
	select {
	case err := <-done0:
		t.Fatalf("rank 0 passed post-barrier before rank 1: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	done1 := make(chan error, 1)
	go func() { done1 <- r1.finish(ctx) }()
	if err := <-done0; err != nil {
		t.Fatal(err)
	}
	if err := <-done1; err != nil {
		t.Fatal(err)
	}

	release, err := t0.enterDataOp(context.Background())
	if err != nil {
		t.Fatalf("rank 0 admission after maintenance = %v", err)
	}
	release()
	release, err = t1.enterDataOp(context.Background())
	if err != nil {
		t.Fatalf("rank 1 admission after maintenance = %v", err)
	}
	release()
}

func newMaintenanceTransportPair(t *testing.T, ctx context.Context) (*daemonTransport, *daemonTransport) {
	t.Helper()
	addr := unusedTCPAddr(t)
	type result struct {
		c   *tcpchan.Channel
		err error
	}
	r0 := make(chan result, 1)
	go func() {
		c, err := tcpchan.New(ctx, 0, 2, addr)
		r0 <- result{c: c, err: err}
	}()
	time.Sleep(10 * time.Millisecond)
	c1, err := tcpchan.New(ctx, 1, 2, addr)
	if err != nil {
		t.Fatal(err)
	}
	res := <-r0
	if res.err != nil {
		t.Fatal(res.err)
	}
	t.Cleanup(func() {
		_ = res.c.Close()
		_ = c1.Close()
	})
	return &daemonTransport{rank: 0, size: 2, side: res.c, admission: newAdmissionGate()},
		&daemonTransport{rank: 1, size: 2, side: c1, admission: newAdmissionGate()}
}

func unusedTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}
