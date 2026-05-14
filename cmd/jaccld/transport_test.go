package main

import (
	"context"
	"fmt"
	"testing"
	"unsafe"

	"github.com/tmc/gojaccl/internal/allocator"
	"github.com/tmc/gojaccl/internal/ipc"
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
