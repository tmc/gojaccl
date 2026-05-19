package jaccl

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestBarrier(t *testing.T) {
	t.Run("FakeTwoRankBarrier", func(t *testing.T) {
		net := newFakeNetwork(2)
		g0 := newFakeGroup(0, 2, net)
		g1 := newFakeGroup(1, 2, net)
		errc := make(chan error, 2)
		go func() { errc <- g0.Barrier(context.Background()) }()
		go func() { errc <- g1.Barrier(context.Background()) }()
		for i := 0; i < 2; i++ {
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("ContextCancellation", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if err := g.Barrier(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Barrier = %v, want deadline", err)
		}
	})
	t.Run("ErrorIncludesRank", func(t *testing.T) {
		g := newFakeGroup(1, 2, newFakeNetwork(2))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := g.Barrier(ctx)
		if err == nil || !strings.Contains(err.Error(), "rank 1 barrier") {
			t.Fatalf("Barrier error = %v, want rank and op", err)
		}
	})
	t.Run("ReturnsClosedGroupError", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		_ = g.Close()
		if err := g.Barrier(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("Barrier closed = %v, want ErrClosed", err)
		}
	})
}

func TestSendRecv(t *testing.T) {
	t.Run("OpaqueBytes", func(t *testing.T) {
		net := newFakeNetwork(2)
		g0 := newFakeGroup(0, 2, net)
		g1 := newFakeGroup(1, 2, net)
		msg := []byte{0, 1, 2, 255}
		dst := make([]byte, len(msg))
		errc := make(chan error, 2)
		go func() { errc <- g0.Send(context.Background(), 1, msg) }()
		go func() { errc <- g1.Recv(context.Background(), 0, dst) }()
		for i := 0; i < 2; i++ {
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(dst, msg) {
			t.Fatalf("recv = %v, want %v", dst, msg)
		}
	})
	t.Run("ZeroLengthPayload", func(t *testing.T) {
		net := newFakeNetwork(2)
		g0 := newFakeGroup(0, 2, net)
		g1 := newFakeGroup(1, 2, net)
		errc := make(chan error, 2)
		go func() { errc <- g0.Send(context.Background(), 1, nil) }()
		go func() { errc <- g1.Recv(context.Background(), 0, nil) }()
		for i := 0; i < 2; i++ {
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("InvalidDestinationRank", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		if err := g.Send(context.Background(), 2, nil); err == nil {
			t.Fatal("Send invalid rank = nil")
		}
	})
	t.Run("InvalidSourceRank", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		if err := g.Recv(context.Background(), -1, nil); err == nil {
			t.Fatal("Recv invalid rank = nil")
		}
	})
	t.Run("ContextCancellation", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if err := g.Send(ctx, 1, []byte("blocked")); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Send blocked = %v, want deadline", err)
		}
	})
	t.Run("ShortReceiveBuffer", func(t *testing.T) {
		net := newFakeNetwork(2)
		g0 := newFakeGroup(0, 2, net)
		g1 := newFakeGroup(1, 2, net)
		errc := make(chan error, 2)
		go func() { errc <- g0.Send(context.Background(), 1, []byte("long")) }()
		go func() { errc <- g1.Recv(context.Background(), 0, make([]byte, 2)) }()
		var got error
		for i := 0; i < 2; i++ {
			if err := <-errc; err != nil {
				got = err
			}
		}
		if got == nil || !strings.Contains(got.Error(), "want 4") {
			t.Fatalf("short recv error = %v", got)
		}
	})
	t.Run("RingRejectsNonNeighborSend", func(t *testing.T) {
		net := newFakeNetwork(4)
		g := newFakeTopologyGroup(0, fakeRingMatrix(4), net, true)
		if err := g.Send(context.Background(), 2, nil); err == nil {
			t.Fatal("ring Send to non-neighbor = nil")
		}
	})
	t.Run("RingRejectsNonNeighborRecv", func(t *testing.T) {
		net := newFakeNetwork(4)
		g := newFakeTopologyGroup(0, fakeRingMatrix(4), net, true)
		if err := g.Recv(context.Background(), 2, nil); err == nil {
			t.Fatal("ring Recv from non-neighbor = nil")
		}
	})
	t.Run("LineRejectsEndpointToEndpointSend", func(t *testing.T) {
		net := newFakeNetwork(3)
		g := newFakeTopologyGroup(0, lineDeviceMatrix("left", "right"), net, true)
		if err := g.Send(context.Background(), 2, nil); err == nil {
			t.Fatal("line Send endpoint to endpoint = nil")
		}
	})
	t.Run("LineRejectsEndpointToEndpointRecv", func(t *testing.T) {
		net := newFakeNetwork(3)
		g := newFakeTopologyGroup(2, lineDeviceMatrix("left", "right"), net, true)
		if err := g.Recv(context.Background(), 0, nil); err == nil {
			t.Fatal("line Recv endpoint from endpoint = nil")
		}
	})
	t.Run("ConnectedRejectsMissingDirectSend", func(t *testing.T) {
		net := newFakeNetwork(4)
		g := newFakeTopologyGroup(0, fakePartialMatrix(), net, true)
		if err := g.Send(context.Background(), 3, nil); err == nil {
			t.Fatal("connected Send over missing direct edge = nil")
		}
	})
}
