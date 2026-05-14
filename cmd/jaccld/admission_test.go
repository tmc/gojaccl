package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/tmc/gojaccl/internal/rdma"
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

func TestDaemonTransportMaintenanceBlocksBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t0, t1 := newMaintenanceTransportPair(t, ctx)

	end, err := t0.beginMaintenance(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	barrierCtx, cancelBarrier := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- t0.Barrier(barrierCtx)
	}()

	peerCtx, cancelPeer := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelPeer()
	if err := t1.Barrier(peerCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("peer barrier while rank 0 is in maintenance = %v, want context deadline", err)
	}
	cancelBarrier()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("rank 0 barrier during maintenance = %v, want context canceled", err)
	}
	end()
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

func TestDaemonTransportMaintainPostsDataQPWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t0, t1 := newMaintenanceTransportPair(t, ctx)
	calls0 := configureMaintenanceTransport(t, t0, 1)
	calls1 := configureMaintenanceTransport(t, t1, 0)

	errc := make(chan error, 2)
	go func() { errc <- t0.Maintain(ctx) }()
	go func() { errc <- t1.Maintain(ctx) }()
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}

	want0 := []maintenanceCall{
		{op: "recv", offset: 3, length: 1, id: transportWorkID(daemonWorkHeartbeatRecv, 1)},
		{op: "send", offset: 2, length: 1, id: transportWorkID(daemonWorkHeartbeatSend, 1)},
	}
	want1 := []maintenanceCall{
		{op: "recv", offset: 1, length: 1, id: transportWorkID(daemonWorkHeartbeatRecv, 0)},
		{op: "send", offset: 0, length: 1, id: transportWorkID(daemonWorkHeartbeatSend, 0)},
	}
	if got := *calls0; !sameMaintenanceCalls(got, want0) {
		t.Fatalf("rank 0 maintenance calls = %+v, want %+v", got, want0)
	}
	if got := *calls1; !sameMaintenanceCalls(got, want1) {
		t.Fatalf("rank 1 maintenance calls = %+v, want %+v", got, want1)
	}
	if t0.conns[1].maintenanceOps.Load() != 1 || t1.conns[0].maintenanceOps.Load() != 1 {
		t.Fatalf("maintenance counters rank0=%d rank1=%d, want one op each", t0.conns[1].maintenanceOps.Load(), t1.conns[0].maintenanceOps.Load())
	}
}

func TestDaemonTransportMaintainPoisonsUnexpectedCompletion(t *testing.T) {
	tp := newSinglePeerMaintenanceTransport(t, 0, 1)
	tp.postRecvWork = func(*rdma.QueuePair, *rdma.MemoryRegion, int, int, uint64) error {
		return nil
	}
	tp.postSendWork = func(*rdma.QueuePair, *rdma.MemoryRegion, int, int, uint64) error {
		return nil
	}
	tp.pollCompletion = func(context.Context, *rdma.CompletionQueue) ([]rdma.WorkRequest, error) {
		return []rdma.WorkRequest{{ID: transportWorkID(daemonWorkSend, 1)}}, nil
	}

	err := tp.runMaintenanceLocked(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unexpected maintenance completion") {
		t.Fatalf("runMaintenanceLocked = %v, want unexpected completion", err)
	}
	if err := tp.conns[1].checkReady(1); err == nil || !strings.Contains(err.Error(), "unexpected maintenance completion") {
		t.Fatalf("checkReady after unexpected completion = %v, want poison", err)
	}
	if tp.conns[1].maintenanceErrs.Load() != 1 {
		t.Fatalf("maintenance errors = %d, want 1", tp.conns[1].maintenanceErrs.Load())
	}
}

func TestDaemonTransportMaintainRunsPostBarrierAfterFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t0, t1 := newMaintenanceTransportPair(t, ctx)
	configureMaintenanceTransport(t, t0, 1)
	configureMaintenanceTransport(t, t1, 0)
	t0.pollCompletion = func(context.Context, *rdma.CompletionQueue) ([]rdma.WorkRequest, error) {
		return []rdma.WorkRequest{{ID: transportWorkID(daemonWorkSend, 1)}}, nil
	}

	errc := make(chan error, 2)
	go func() { errc <- t0.Maintain(ctx) }()
	go func() { errc <- t1.Maintain(ctx) }()

	var gotUnexpected, gotNil bool
	for i := 0; i < 2; i++ {
		err := <-errc
		switch {
		case err == nil:
			gotNil = true
		case strings.Contains(err.Error(), "unexpected maintenance completion"):
			gotUnexpected = true
		default:
			t.Fatalf("Maintain error = %v, want nil or unexpected completion", err)
		}
	}
	if !gotUnexpected || !gotNil {
		t.Fatalf("Maintain results unexpected=%v nil=%v, want one of each", gotUnexpected, gotNil)
	}
}

func TestDaemonTransportMaintainPoisonsAllPeersOnPartialPost(t *testing.T) {
	slab := newTransportTestSlab(t)
	lease, err := slab.Alloc(maintenanceBytes(3))
	if err != nil {
		t.Fatal(err)
	}
	conns := []*daemonConn{nil, &daemonConn{}, &daemonConn{}}
	tp := &daemonTransport{
		rank:             0,
		size:             3,
		slab:             slab,
		maintenanceLease: lease,
		conns:            conns,
	}
	tp.postRecvWork = func(_ *rdma.QueuePair, _ *rdma.MemoryRegion, offset, length int, id uint64) error {
		if id == transportWorkID(daemonWorkHeartbeatRecv, 2) {
			return errors.New("post failed")
		}
		return nil
	}

	err = tp.runMaintenanceLocked(context.Background())
	if err == nil || !strings.Contains(err.Error(), "post failed") {
		t.Fatalf("runMaintenanceLocked = %v, want post failure", err)
	}
	for peer := 1; peer <= 2; peer++ {
		if err := tp.conns[peer].checkReady(peer); err == nil || !strings.Contains(err.Error(), "post failed") {
			t.Fatalf("peer %d checkReady = %v, want poison", peer, err)
		}
		if tp.conns[peer].maintenanceErrs.Load() != 1 {
			t.Fatalf("peer %d maintenance errors = %d, want 1", peer, tp.conns[peer].maintenanceErrs.Load())
		}
	}
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

type maintenanceCall struct {
	op     string
	offset int
	length int
	id     uint64
}

func configureMaintenanceTransport(t *testing.T, tp *daemonTransport, peer int) *[]maintenanceCall {
	t.Helper()
	slab := newTransportTestSlab(t)
	lease, err := slab.Alloc(maintenanceBytes(tp.size))
	if err != nil {
		t.Fatal(err)
	}
	tp.slab = slab
	tp.maintenanceLease = lease
	tp.conns = make([]*daemonConn, tp.size)
	tp.conns[peer] = &daemonConn{}
	var calls []maintenanceCall
	tp.postRecvWork = func(_ *rdma.QueuePair, _ *rdma.MemoryRegion, offset, length int, id uint64) error {
		calls = append(calls, maintenanceCall{op: "recv", offset: offset, length: length, id: id})
		return nil
	}
	tp.postSendWork = func(_ *rdma.QueuePair, _ *rdma.MemoryRegion, offset, length int, id uint64) error {
		calls = append(calls, maintenanceCall{op: "send", offset: offset, length: length, id: id})
		return nil
	}
	tp.pollCompletion = func(context.Context, *rdma.CompletionQueue) ([]rdma.WorkRequest, error) {
		return []rdma.WorkRequest{
			{ID: transportWorkID(daemonWorkHeartbeatRecv, peer)},
			{ID: transportWorkID(daemonWorkHeartbeatSend, peer)},
		}, nil
	}
	return &calls
}

func newSinglePeerMaintenanceTransport(t *testing.T, rank, peer int) *daemonTransport {
	t.Helper()
	slab := newTransportTestSlab(t)
	lease, err := slab.Alloc(maintenanceBytes(2))
	if err != nil {
		t.Fatal(err)
	}
	conns := make([]*daemonConn, 2)
	conns[peer] = &daemonConn{}
	return &daemonTransport{
		rank:             rank,
		size:             2,
		slab:             slab,
		maintenanceLease: lease,
		conns:            conns,
	}
}

func sameMaintenanceCalls(a, b []maintenanceCall) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
