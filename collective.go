package jaccl

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/tmc/gojaccl/internal/reduce"
)

// Element is a value that JACCL can move through typed collectives.
type Element interface {
	bool | int8 | int16 | int32 | int64 |
		uint8 | uint16 | uint32 | uint64 |
		float32 | float64 | complex64
}

// AllSum computes the element-wise sum across all ranks.
func AllSum[T Element](ctx context.Context, g *Group, dst, src []T) error {
	return allReduce(ctx, g, "all sum", reductionSum, dst, src)
}

// AllMax computes the element-wise maximum across all ranks.
func AllMax[T Element](ctx context.Context, g *Group, dst, src []T) error {
	return allReduce(ctx, g, "all max", reductionMax, dst, src)
}

// AllMin computes the element-wise minimum across all ranks.
func AllMin[T Element](ctx context.Context, g *Group, dst, src []T) error {
	return allReduce(ctx, g, "all min", reductionMin, dst, src)
}

// AllGather gathers one slice from each rank into dst in rank order.
func AllGather[T Element](ctx context.Context, g *Group, dst, src []T) error {
	if g == nil {
		return wrapError(-1, "all gather", ErrClosed)
	}
	want := g.Size() * len(src)
	if len(dst) != want {
		return wrapError(g.Rank(), "all gather", fmt.Errorf("destination length %d, want %d", len(dst), want))
	}
	if slicesOverlap(dst, src) {
		return wrapError(g.Rank(), "all gather", fmt.Errorf("source and destination overlap"))
	}
	return g.do(ctx, "all gather", func(b backend) error {
		return b.allGather(ctx, elementSize[T](), sliceBytes(dst), sliceBytes(src))
	})
}

func allReduce[T Element](ctx context.Context, g *Group, name string, op reductionOp, dst, src []T) error {
	if g == nil {
		return wrapError(-1, name, ErrClosed)
	}
	if len(dst) != len(src) {
		return wrapError(g.Rank(), name, fmt.Errorf("destination length %d, want %d", len(dst), len(src)))
	}
	dt, err := reduce.DTypeFor[T]()
	if err != nil {
		return wrapError(g.Rank(), name, err)
	}
	return g.do(ctx, name, func(b backend) error {
		return b.allReduce(ctx, op, dt, sliceBytes(dst), sliceBytes(src))
	})
}

func elementSize[T any]() int {
	var zero T
	return int(unsafe.Sizeof(zero))
}

func sliceBytes[T any](s []T) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&s[0])), len(s)*elementSize[T]())
}

func slicesOverlap[T any](a, b []T) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	size := uintptr(elementSize[T]())
	a0 := uintptr(unsafe.Pointer(&a[0]))
	a1 := a0 + uintptr(len(a))*size
	b0 := uintptr(unsafe.Pointer(&b[0]))
	b1 := b0 + uintptr(len(b))*size
	return a0 < b1 && b0 < a1
}
