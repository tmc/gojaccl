package resource

import (
	"context"
	"errors"
	"testing"

	"github.com/tmc/gojaccl/internal/allocator"
)

func TestSlabMRPool(t *testing.T) {
	slab, err := allocator.NewSlab(t.TempDir(), 64)
	if err != nil {
		t.Fatal(err)
	}
	defer slab.Close()
	pool, err := NewSlabMRPool(slab)
	if err != nil {
		t.Fatal(err)
	}
	w, err := pool.AllocMR(context.Background(), 16)
	if err != nil {
		t.Fatal(err)
	}
	if w.SlabID == "" || w.Offset != 0 || w.Length != 16 {
		t.Fatalf("window = %+v, want slab id offset 0 length 16", w)
	}
	if stats := pool.MRStats(); stats.InUse != 1 || stats.BytesInUse != 16 {
		t.Fatalf("stats after alloc = %+v, want one 16-byte window", stats)
	}
	if err := pool.FreeMR(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if stats := pool.MRStats(); stats.InUse != 0 || stats.BytesInUse != 0 {
		t.Fatalf("stats after free = %+v, want empty", stats)
	}
	if err := pool.FreeMR(context.Background(), MRWindow{SlabID: "bad"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("FreeMR bad id = %v, want ErrInvalidRequest", err)
	}
}

func TestStaticQueuePairPool(t *testing.T) {
	pool, err := NewStaticQueuePairPool([]QueuePairHandle{"qp0"})
	if err != nil {
		t.Fatal(err)
	}
	h, err := pool.AcquireQueuePair(context.Background(), PeerSpec{Rank: 1})
	if err != nil {
		t.Fatal(err)
	}
	if h != "qp0" {
		t.Fatalf("handle = %q, want qp0", h)
	}
	if _, err := pool.AcquireQueuePair(context.Background(), PeerSpec{Rank: 2}); !errors.Is(err, ErrExhausted) {
		t.Fatalf("Acquire exhausted = %v, want ErrExhausted", err)
	}
	if err := pool.ReleaseQueuePair(context.Background(), h); err != nil {
		t.Fatal(err)
	}
	if stats := pool.QueuePairStats(); stats.InUse != 0 || stats.Available != 1 {
		t.Fatalf("stats after release = %+v, want one available", stats)
	}
}

func TestStaticCompletionQueuePool(t *testing.T) {
	pool, err := NewStaticCompletionQueuePool([]CompletionQueueHandle{"cq0"})
	if err != nil {
		t.Fatal(err)
	}
	h, err := pool.AcquireCompletionQueue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if h != "cq0" {
		t.Fatalf("handle = %q, want cq0", h)
	}
	if _, err := pool.AcquireCompletionQueue(context.Background()); !errors.Is(err, ErrExhausted) {
		t.Fatalf("Acquire exhausted = %v, want ErrExhausted", err)
	}
	if err := pool.ReleaseCompletionQueue(context.Background(), h); err != nil {
		t.Fatal(err)
	}
	if stats := pool.CompletionQueueStats(); stats.InUse != 0 || stats.Available != 1 {
		t.Fatalf("stats after release = %+v, want one available", stats)
	}
}
