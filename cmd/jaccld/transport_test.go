package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/tmc/gojaccl/internal/allocator"
	"github.com/tmc/gojaccl/internal/ipc"
	"github.com/tmc/gojaccl/internal/jaccld/resource"
	"github.com/tmc/gojaccl/internal/reduce"
)

var _ ipc.CollectiveTransport = (*daemonTransport)(nil)

func TestDaemonTransportMeshReduceOffline(t *testing.T) {
	slab := newTransportTestSlab(t)
	tp := &daemonTransport{rank: 1, size: 3, slab: slab}
	src := allocBytes(t, slab, int64Bytes([]int64{5}))
	dst := allocBytes(t, slab, make([]byte, 8))
	peers := map[int][]byte{
		0: int64Bytes([]int64{3}),
		2: int64Bytes([]int64{7}),
	}
	exchange := func(ctx context.Context, dstBase, srcOffset, length int64) error {
		for peer, data := range peers {
			copy(slab.Bytes()[peerOffset(dstBase, peer, length):peerOffset(dstBase, peer, length)+length], data)
		}
		return nil
	}

	if err := tp.runMeshReduce(context.Background(), daemonReductionSum, reduce.Int64, dst.Offset, dst.Length, src.Offset, src.Length, exchange); err != nil {
		t.Fatal(err)
	}
	if got := bytesAsInt64s(slab.Bytes()[dst.Offset : dst.Offset+dst.Length]); fmt.Sprint(got) != "[15]" {
		t.Fatalf("sum dst = %v, want [15]", got)
	}
	if stats := slab.Stats(); stats.Leases != 2 {
		t.Fatalf("stats after reduce = %+v, want only src and dst leases", stats)
	}
}

func TestDaemonTransportMeshGatherOffline(t *testing.T) {
	slab := newTransportTestSlab(t)
	tp := &daemonTransport{rank: 1, size: 3, slab: slab}
	src := allocBytes(t, slab, int64Bytes([]int64{11}))
	dst := allocBytes(t, slab, make([]byte, 3*8))
	peers := map[int][]byte{
		0: int64Bytes([]int64{3}),
		2: int64Bytes([]int64{7}),
	}
	exchange := func(ctx context.Context, dstBase, srcOffset, length int64) error {
		for peer, data := range peers {
			copy(slab.Bytes()[peerOffset(dstBase, peer, length):peerOffset(dstBase, peer, length)+length], data)
		}
		return nil
	}

	if err := tp.runMeshGather(context.Background(), 8, dst.Offset, dst.Length, src.Offset, src.Length, exchange); err != nil {
		t.Fatal(err)
	}
	got := bytesAsInt64s(slab.Bytes()[dst.Offset : dst.Offset+dst.Length])
	if fmt.Sprint(got) != "[3 11 7]" {
		t.Fatalf("gather dst = %v, want [3 11 7]", got)
	}
	if stats := slab.Stats(); stats.Leases != 2 {
		t.Fatalf("stats after gather = %+v, want only src and dst leases", stats)
	}
}

func TestDaemonTransportCollectiveValidation(t *testing.T) {
	slab := newTransportTestSlab(t)
	tp := &daemonTransport{rank: 0, size: 2, slab: slab}
	src := allocBytes(t, slab, int64Bytes([]int64{1}))
	dst := allocBytes(t, slab, make([]byte, 8))

	if err := tp.runMeshReduce(context.Background(), 99, reduce.Int64, dst.Offset, dst.Length, src.Offset, src.Length, func(ctx context.Context, dstBase, srcOffset, length int64) error {
		copy(slab.Bytes()[peerOffset(dstBase, 1, length):peerOffset(dstBase, 2, length)], int64Bytes([]int64{2}))
		return nil
	}); err == nil {
		t.Fatal("runMeshReduce with unknown op = nil")
	}
	if err := tp.runMeshGather(context.Background(), 0, dst.Offset, dst.Length, src.Offset, src.Length, nil); err == nil {
		t.Fatal("runMeshGather with zero element size = nil")
	}
	if err := tp.runMeshGather(context.Background(), 8, dst.Offset, dst.Length, src.Offset, src.Length, nil); err == nil {
		t.Fatal("runMeshGather with short destination = nil")
	}
}

func TestHeartbeatMRFromLease(t *testing.T) {
	mr, err := heartbeatMR(100, 9, allocator.Lease{ID: 7, Offset: 5, Length: 1}, 11)
	if err != nil {
		t.Fatal(err)
	}
	want := resource.HeartbeatMR{Addr: 105, RKey: 9, Length: 1, Epoch: 11}
	if mr != want {
		t.Fatalf("heartbeatMR = %+v, want %+v", mr, want)
	}

	tests := []struct {
		name  string
		base  uint64
		rkey  uint32
		lease allocator.Lease
		epoch uint64
	}{
		{name: "ZeroBase", rkey: 1, lease: allocator.Lease{ID: 1, Length: 1}, epoch: 1},
		{name: "ZeroRKey", base: 1, lease: allocator.Lease{ID: 1, Length: 1}, epoch: 1},
		{name: "ZeroLength", base: 1, rkey: 1, lease: allocator.Lease{ID: 1}, epoch: 1},
		{name: "ZeroEpoch", base: 1, rkey: 1, lease: allocator.Lease{ID: 1, Length: 1}},
		{name: "NegativeOffset", base: 1, rkey: 1, lease: allocator.Lease{ID: 1, Offset: -1, Length: 1}, epoch: 1},
		{name: "Overflow", base: ^uint64(0), rkey: 1, lease: allocator.Lease{ID: 1, Offset: 1, Length: 1}, epoch: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := heartbeatMR(tt.base, tt.rkey, tt.lease, tt.epoch); err == nil {
				t.Fatal("heartbeatMR = nil, want error")
			}
		})
	}
}

func TestValidateRemoteHeartbeatDestination(t *testing.T) {
	now := time.Unix(100, 0)
	dst := daemonDestination{
		HeartbeatMR:  resource.HeartbeatMR{Addr: 100, RKey: 9, Length: 1, Epoch: 11},
		HeartbeatTTL: time.Minute,
	}
	lease, err := validateRemoteHeartbeatDestination(dst, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if lease.MR != dst.HeartbeatMR {
		t.Fatalf("heartbeat lease mr = %+v, want %+v", lease.MR, dst.HeartbeatMR)
	}
	if !lease.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("heartbeat expiry = %s, want %s", lease.ExpiresAt, now.Add(time.Minute))
	}
	if _, err := validateRemoteHeartbeatDestination(dst, now, 11); !errors.Is(err, resource.ErrInvalidRequest) {
		t.Fatalf("stale epoch = %v, want ErrInvalidRequest", err)
	}
	dst.HeartbeatTTL = 0
	if _, err := validateRemoteHeartbeatDestination(dst, now, 0); !errors.Is(err, resource.ErrInvalidRequest) {
		t.Fatalf("zero ttl = %v, want ErrInvalidRequest", err)
	}
	dst.HeartbeatTTL = time.Minute
	dst.HeartbeatMR.RKey = 0
	if _, err := validateRemoteHeartbeatDestination(dst, now, 0); !errors.Is(err, resource.ErrInvalidRequest) {
		t.Fatalf("zero rkey = %v, want ErrInvalidRequest", err)
	}
}

func TestDaemonHeartbeatLeaseRDMAExpires(t *testing.T) {
	now := time.Unix(100, 0)
	lease := daemonHeartbeatLease{
		MR:        resource.HeartbeatMR{Addr: 100, RKey: 9, Length: 1, Epoch: 11},
		ExpiresAt: now.Add(time.Minute),
	}
	if _, err := lease.RDMA(now); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.RDMA(now.Add(time.Minute)); !errors.Is(err, resource.ErrExpired) {
		t.Fatalf("expired RDMA = %v, want ErrExpired", err)
	}
}

func newTransportTestSlab(t *testing.T) *allocator.Slab {
	t.Helper()
	slab, err := allocator.NewSlab(t.TempDir(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := slab.Close(); err != nil {
			t.Errorf("close slab: %v", err)
		}
	})
	return slab
}

func allocBytes(t *testing.T, slab *allocator.Slab, data []byte) allocator.Lease {
	t.Helper()
	lease, err := slab.Alloc(int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	copy(slab.Bytes()[lease.Offset:lease.Offset+lease.Length], data)
	return lease
}

func int64Bytes(v []int64) []byte {
	if len(v) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&v[0])), len(v)*8)
}

func bytesAsInt64s(b []byte) []int64 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*int64)(unsafe.Pointer(&b[0])), len(b)/8)
}
