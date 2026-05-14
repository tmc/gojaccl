package resource

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestStoreOpenClose(t *testing.T) {
	store, pools := newTestStore(t)
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	if err := store.SetState(StateReady); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Open(context.Background(), SessionRequest{
		ClientID: "client-1",
		Peer:     PeerSpec{Rank: 1},
		Size:     64,
		Deadline: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if lease.ID == 0 {
		t.Fatal("lease id is zero")
	}
	if lease.Window.Offset != 0 || lease.Window.Length != 64 {
		t.Fatalf("lease window = %+v, want offset 0 length 64", lease.Window)
	}
	if lease.QueuePair == "" || lease.CompletionQueue == "" {
		t.Fatalf("lease handles = %q %q, want non-empty", lease.QueuePair, lease.CompletionQueue)
	}
	if !lease.LastActivity.Equal(now) || !lease.Healthy {
		t.Fatalf("lease liveness = %s %v, want %s true", lease.LastActivity, lease.Healthy, now)
	}
	if stats := store.Stats(); stats.Leases != 1 || stats.MemoryRegions.BytesInUse != 64 || stats.QueuePairs.InUse != 1 || stats.CompletionQueues.InUse != 1 {
		t.Fatalf("stats after open = %+v, want one lease", stats)
	}
	if err := store.Close(context.Background(), lease.ID); err != nil {
		t.Fatal(err)
	}
	if stats := store.Stats(); stats.Leases != 0 || stats.MemoryRegions.BytesInUse != 0 || stats.QueuePairs.InUse != 0 || stats.CompletionQueues.InUse != 0 {
		t.Fatalf("stats after close = %+v, want empty", stats)
	}
	if len(pools.mr.used) != 0 || len(pools.qp.used) != 0 || len(pools.cq.used) != 0 {
		t.Fatalf("fake pools still have resources: mr=%d qp=%d cq=%d", len(pools.mr.used), len(pools.qp.used), len(pools.cq.used))
	}
}

func TestStoreNeedsReady(t *testing.T) {
	store, _ := newTestStore(t)
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	_, err := store.Open(context.Background(), SessionRequest{
		ClientID: "client-1",
		Peer:     PeerSpec{Rank: 1},
		Size:     1,
		Deadline: now.Add(time.Minute),
	})
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("Open before ready = %v, want ErrNotReady", err)
	}
}

func TestStoreRejectsBadRequests(t *testing.T) {
	now := time.Unix(100, 0)
	tests := []struct {
		name string
		req  SessionRequest
		want error
	}{
		{
			name: "EmptyClient",
			req:  SessionRequest{Peer: PeerSpec{Rank: 1}, Size: 1, Deadline: now.Add(time.Minute)},
			want: ErrInvalidRequest,
		},
		{
			name: "NegativePeer",
			req:  SessionRequest{ClientID: "client", Peer: PeerSpec{Rank: -1}, Size: 1, Deadline: now.Add(time.Minute)},
			want: ErrInvalidRequest,
		},
		{
			name: "ZeroSize",
			req:  SessionRequest{ClientID: "client", Peer: PeerSpec{Rank: 1}, Deadline: now.Add(time.Minute)},
			want: ErrInvalidRequest,
		},
		{
			name: "ZeroDeadline",
			req:  SessionRequest{ClientID: "client", Peer: PeerSpec{Rank: 1}, Size: 1},
			want: ErrInvalidRequest,
		},
		{
			name: "ExpiredDeadline",
			req:  SessionRequest{ClientID: "client", Peer: PeerSpec{Rank: 1}, Size: 1, Deadline: now},
			want: ErrExpired,
		},
		{
			name: "NegativeHeartbeat",
			req:  SessionRequest{ClientID: "client", Peer: PeerSpec{Rank: 1}, Size: 1, Deadline: now.Add(time.Minute), Heartbeat: -time.Second},
			want: ErrInvalidRequest,
		},
		{
			name: "ZeroHeartbeatAddr",
			req:  SessionRequest{ClientID: "client", Peer: PeerSpec{Rank: 1}, Size: 1, Deadline: now.Add(time.Minute), HeartbeatMR: HeartbeatMR{RKey: 1, Length: 1, Epoch: 1}},
			want: ErrInvalidRequest,
		},
		{
			name: "ZeroHeartbeatRKey",
			req:  SessionRequest{ClientID: "client", Peer: PeerSpec{Rank: 1}, Size: 1, Deadline: now.Add(time.Minute), HeartbeatMR: HeartbeatMR{Addr: 1, Length: 1, Epoch: 1}},
			want: ErrInvalidRequest,
		},
		{
			name: "ZeroHeartbeatLength",
			req:  SessionRequest{ClientID: "client", Peer: PeerSpec{Rank: 1}, Size: 1, Deadline: now.Add(time.Minute), HeartbeatMR: HeartbeatMR{Addr: 1, RKey: 1, Epoch: 1}},
			want: ErrInvalidRequest,
		},
		{
			name: "ZeroHeartbeatEpoch",
			req:  SessionRequest{ClientID: "client", Peer: PeerSpec{Rank: 1}, Size: 1, Deadline: now.Add(time.Minute), HeartbeatMR: HeartbeatMR{Addr: 1, RKey: 1, Length: 1}},
			want: ErrInvalidRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, _ := newTestStore(t)
			store.now = func() time.Time { return now }
			if err := store.SetState(StateReady); err != nil {
				t.Fatal(err)
			}
			_, err := store.Open(context.Background(), tt.req)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Open = %v, want %v", err, tt.want)
			}
			if stats := store.Stats(); stats.Leases != 0 || stats.MemoryRegions.BytesInUse != 0 || stats.QueuePairs.InUse != 0 || stats.CompletionQueues.InUse != 0 {
				t.Fatalf("stats after rejected request = %+v, want empty", stats)
			}
		})
	}
}

func TestHeartbeatMRValidation(t *testing.T) {
	tests := []struct {
		name string
		mr   HeartbeatMR
		want error
	}{
		{name: "Valid", mr: testHeartbeatMR(), want: nil},
		{name: "ZeroAddr", mr: HeartbeatMR{RKey: 2, Length: 1, Epoch: 1}, want: ErrInvalidRequest},
		{name: "ZeroRKey", mr: HeartbeatMR{Addr: 1, Length: 1, Epoch: 1}, want: ErrInvalidRequest},
		{name: "ZeroLength", mr: HeartbeatMR{Addr: 1, RKey: 2, Epoch: 1}, want: ErrInvalidRequest},
		{name: "NegativeLength", mr: HeartbeatMR{Addr: 1, RKey: 2, Length: -1, Epoch: 1}, want: ErrInvalidRequest},
		{name: "ZeroEpoch", mr: HeartbeatMR{Addr: 1, RKey: 2, Length: 1}, want: ErrInvalidRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.mr.ValidateForRDMA()
			if !errors.Is(err, tt.want) {
				t.Fatalf("ValidateForRDMA = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestStoreHeartbeatMRLease(t *testing.T) {
	store, _ := newTestStore(t)
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	if err := store.SetState(StateReady); err != nil {
		t.Fatal(err)
	}
	hb := testHeartbeatMR()
	lease, err := store.Open(context.Background(), SessionRequest{
		ClientID:    "client",
		Peer:        PeerSpec{Rank: 1},
		Size:        8,
		Deadline:    now.Add(time.Minute),
		HeartbeatMR: hb,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := lease.RDMAHeartbeatMR(now)
	if err != nil {
		t.Fatal(err)
	}
	if got != hb {
		t.Fatalf("RDMAHeartbeatMR = %+v, want %+v", got, hb)
	}
	if _, err := lease.RDMAHeartbeatMR(now.Add(time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired RDMAHeartbeatMR = %v, want ErrExpired", err)
	}
}

func TestStoreExhaustionCleansUp(t *testing.T) {
	store, pools := newTestStore(t)
	pools.qp.capacity = 0
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	if err := store.SetState(StateReady); err != nil {
		t.Fatal(err)
	}
	_, err := store.Open(context.Background(), SessionRequest{
		ClientID: "client-1",
		Peer:     PeerSpec{Rank: 1},
		Size:     64,
		Deadline: now.Add(time.Minute),
	})
	if !errors.Is(err, ErrExhausted) {
		t.Fatalf("Open with exhausted qp pool = %v, want ErrExhausted", err)
	}
	if stats := store.Stats(); stats.Leases != 0 || stats.MemoryRegions.BytesInUse != 0 || stats.QueuePairs.InUse != 0 || stats.CompletionQueues.InUse != 0 {
		t.Fatalf("stats after exhaustion = %+v, want empty", stats)
	}
}

func TestStoreReapsExpired(t *testing.T) {
	store, _ := newTestStore(t)
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	if err := store.SetState(StateReady); err != nil {
		t.Fatal(err)
	}
	oldLease, err := store.Open(context.Background(), SessionRequest{
		ClientID: "old",
		Peer:     PeerSpec{Rank: 1},
		Size:     8,
		Deadline: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	newLease, err := store.Open(context.Background(), SessionRequest{
		ClientID: "new",
		Peer:     PeerSpec{Rank: 2},
		Size:     8,
		Deadline: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	n, err := store.ReapExpired(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("ReapExpired = %d, want 1", n)
	}
	if _, ok := store.Lookup(oldLease.ID); ok {
		t.Fatal("old lease is still live")
	}
	if _, ok := store.Lookup(newLease.ID); !ok {
		t.Fatal("new lease was reaped")
	}
}

func TestStoreRefresh(t *testing.T) {
	store, _ := newTestStore(t)
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	if err := store.SetState(StateReady); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Open(context.Background(), SessionRequest{
		ClientID: "client",
		Peer:     PeerSpec{Rank: 1},
		Size:     8,
		Deadline: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(5 * time.Second)
	refreshed, err := store.Refresh(lease.ID, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !refreshed.ExpiresAt.Equal(now.Add(2 * time.Minute)) {
		t.Fatalf("refreshed deadline = %s, want %s", refreshed.ExpiresAt, now.Add(2*time.Minute))
	}
	if !refreshed.LastActivity.Equal(now) || !refreshed.Healthy {
		t.Fatalf("refreshed liveness = %s %v, want %s true", refreshed.LastActivity, refreshed.Healthy, now)
	}
	if _, err := store.Refresh(lease.ID, now); !errors.Is(err, ErrExpired) {
		t.Fatalf("Refresh expired = %v, want ErrExpired", err)
	}
}

func TestStoreRefreshExpiredLeaseFails(t *testing.T) {
	store, _ := newTestStore(t)
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	if err := store.SetState(StateReady); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Open(context.Background(), SessionRequest{
		ClientID: "client",
		Peer:     PeerSpec{Rank: 1},
		Size:     8,
		Deadline: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if _, err := store.Refresh(lease.ID, now.Add(time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("Refresh expired lease = %v, want ErrExpired", err)
	}
}

func TestStoreTouch(t *testing.T) {
	store, _ := newTestStore(t)
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	if err := store.SetState(StateReady); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Open(context.Background(), SessionRequest{
		ClientID: "client",
		Peer:     PeerSpec{Rank: 1},
		Size:     8,
		Deadline: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	touched, err := store.Touch(lease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !touched.LastActivity.Equal(now) || !touched.ExpiresAt.Equal(lease.ExpiresAt) {
		t.Fatalf("Touch = last %s expiry %s, want last %s expiry %s", touched.LastActivity, touched.ExpiresAt, now, lease.ExpiresAt)
	}
}

func TestStoreControlPlaneLivenessDoesNotRefreshExpiry(t *testing.T) {
	store, _ := newTestStore(t)
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	if err := store.SetState(StateReady); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Open(context.Background(), SessionRequest{
		ClientID: "client",
		Peer:     PeerSpec{Rank: 1},
		Size:     8,
		Deadline: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(500 * time.Millisecond)
	if n := store.PulseControlPlane(); n != 1 {
		t.Fatalf("PulseControlPlane = %d, want 1", n)
	}
	pulsed, ok := store.Lookup(lease.ID)
	if !ok {
		t.Fatal("lease disappeared")
	}
	if !pulsed.LastActivity.Equal(now) {
		t.Fatalf("last activity = %s, want %s", pulsed.LastActivity, now)
	}
	if !pulsed.ExpiresAt.Equal(lease.ExpiresAt) {
		t.Fatalf("expiry = %s, want unchanged %s", pulsed.ExpiresAt, lease.ExpiresAt)
	}
	now = now.Add(time.Second)
	if n := store.PulseControlPlane(); n != 0 {
		t.Fatalf("PulseControlPlane after expiry = %d, want 0", n)
	}
	if _, err := store.Touch(lease.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("Touch expired = %v, want ErrExpired", err)
	}
}

func TestStoreRunControlPlaneLiveness(t *testing.T) {
	store, _ := newTestStore(t)
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	if err := store.SetState(StateReady); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Open(context.Background(), SessionRequest{
		ClientID: "client",
		Peer:     PeerSpec{Rank: 1},
		Size:     8,
		Deadline: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RunControlPlaneLiveness(context.Background(), 0); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("RunControlPlaneLiveness zero interval = %v, want ErrInvalidRequest", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	now = now.Add(time.Second)
	go func() {
		done <- store.RunControlPlaneLiveness(ctx, time.Millisecond)
	}()
	deadline := time.After(time.Second)
	for {
		got, ok := store.Lookup(lease.ID)
		if !ok {
			t.Fatal("lease disappeared")
		}
		if got.LastActivity.Equal(now) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("liveness did not update last activity; got %s want %s", got.LastActivity, now)
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunControlPlaneLiveness = %v, want context.Canceled", err)
	}
}

func TestStoreStateTransitions(t *testing.T) {
	store, _ := newTestStore(t)
	for _, state := range []State{StateOpening, StateReady, StateDraining, StateTerminated} {
		if err := store.SetState(state); err != nil {
			t.Fatalf("SetState(%s): %v", state, err)
		}
	}
	if err := store.SetState(StateReady); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("SetState after terminated = %v, want ErrInvalidState", err)
	}
}

func testHeartbeatMR() HeartbeatMR {
	return HeartbeatMR{Addr: 1, RKey: 2, Length: 1, Epoch: 1}
}

type testPools struct {
	mr *fakeMRPool
	qp *fakeQPPool
	cq *fakeCQPool
}

func newTestStore(t *testing.T) (*Store, testPools) {
	t.Helper()
	pools := testPools{
		mr: newFakeMRPool(256),
		qp: newFakeQPPool(4),
		cq: newFakeCQPool(4),
	}
	store, err := NewStore(pools.mr, pools.qp, pools.cq)
	if err != nil {
		t.Fatal(err)
	}
	return store, pools
}

type fakeMRPool struct {
	total int64
	next  int64
	used  map[string]MRWindow
}

func newFakeMRPool(total int64) *fakeMRPool {
	return &fakeMRPool{total: total, used: make(map[string]MRWindow)}
}

func (p *fakeMRPool) AllocMR(_ context.Context, n int64) (MRWindow, error) {
	if n <= 0 {
		return MRWindow{}, ErrInvalidRequest
	}
	if p.bytesInUse()+n > p.total {
		return MRWindow{}, ErrExhausted
	}
	id := fmt.Sprintf("mr-%d", len(p.used)+1)
	w := MRWindow{SlabID: "test-slab", Offset: p.next, Length: n}
	p.next += n
	p.used[id] = w
	return w, nil
}

func (p *fakeMRPool) FreeMR(_ context.Context, w MRWindow) error {
	for id, used := range p.used {
		if used == w {
			delete(p.used, id)
			return nil
		}
	}
	return ErrLeaseNotFound
}

func (p *fakeMRPool) MRStats() PoolStats {
	used := p.bytesInUse()
	return PoolStats{InUse: len(p.used), Available: -1, BytesInUse: used, BytesAvailable: p.total - used}
}

func (p *fakeMRPool) bytesInUse() int64 {
	var used int64
	for _, w := range p.used {
		used += w.Length
	}
	return used
}

type fakeQPPool struct {
	capacity int
	next     int
	used     map[QueuePairHandle]PeerSpec
}

func newFakeQPPool(capacity int) *fakeQPPool {
	return &fakeQPPool{capacity: capacity, used: make(map[QueuePairHandle]PeerSpec)}
}

func (p *fakeQPPool) AcquireQueuePair(_ context.Context, peer PeerSpec) (QueuePairHandle, error) {
	if len(p.used) >= p.capacity {
		return "", ErrExhausted
	}
	p.next++
	h := QueuePairHandle(fmt.Sprintf("qp-%d", p.next))
	p.used[h] = peer
	return h, nil
}

func (p *fakeQPPool) ReleaseQueuePair(_ context.Context, h QueuePairHandle) error {
	if _, ok := p.used[h]; !ok {
		return ErrLeaseNotFound
	}
	delete(p.used, h)
	return nil
}

func (p *fakeQPPool) QueuePairStats() PoolStats {
	return PoolStats{InUse: len(p.used), Available: p.capacity - len(p.used)}
}

type fakeCQPool struct {
	capacity int
	next     int
	used     map[CompletionQueueHandle]bool
}

func newFakeCQPool(capacity int) *fakeCQPool {
	return &fakeCQPool{capacity: capacity, used: make(map[CompletionQueueHandle]bool)}
}

func (p *fakeCQPool) AcquireCompletionQueue(context.Context) (CompletionQueueHandle, error) {
	if len(p.used) >= p.capacity {
		return "", ErrExhausted
	}
	p.next++
	h := CompletionQueueHandle(fmt.Sprintf("cq-%d", p.next))
	p.used[h] = true
	return h, nil
}

func (p *fakeCQPool) ReleaseCompletionQueue(_ context.Context, h CompletionQueueHandle) error {
	if !p.used[h] {
		return ErrLeaseNotFound
	}
	delete(p.used, h)
	return nil
}

func (p *fakeCQPool) CompletionQueueStats() PoolStats {
	return PoolStats{InUse: len(p.used), Available: p.capacity - len(p.used)}
}
