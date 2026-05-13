package jaccl

import (
	"context"
	"testing"
)

func BenchmarkAllSumFloat32(b *testing.B) {
	for _, bm := range []struct {
		name  string
		elems int
	}{
		{"2Ranks/1KiB", 256},
		{"2Ranks/64KiB", 16 * 1024},
		{"2Ranks/1MiB", 256 * 1024},
		{"2Ranks/InPlace", 256},
	} {
		b.Run(bm.name, func(b *testing.B) {
			g := newFakeGroup(0, 1, newFakeNetwork(1))
			src := make([]float32, bm.elems)
			dst := make([]float32, bm.elems)
			if bm.name == "2Ranks/InPlace" {
				dst = src
			}
			b.SetBytes(int64(len(src) * 4))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := AllSum(context.Background(), g, dst, src); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAllGatherFloat32(b *testing.B) {
	b.Run("2Ranks/1KiB", func(b *testing.B) {
		g := newFakeGroup(0, 1, newFakeNetwork(1))
		src := make([]float32, 256)
		dst := make([]float32, 256)
		b.SetBytes(int64(len(src) * 4))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := AllGather(context.Background(), g, dst, src); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkBarrier(b *testing.B) {
	b.Run("2Ranks", func(b *testing.B) {
		g := newFakeGroup(0, 1, newFakeNetwork(1))
		for i := 0; i < b.N; i++ {
			if err := g.Barrier(context.Background()); err != nil {
				b.Fatal(err)
			}
		}
	})
}
