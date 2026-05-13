package tcpchan

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestSideChannelDial(t *testing.T) {
	t.Run("RankZeroListensAndNonZeroRankConnects", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c0, c1 := newPair(t, ctx)
		defer c0.Close()
		defer c1.Close()
	})
	t.Run("ContextCancelBeforeDial", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := New(ctx, 1, 2, "127.0.0.1:1"); !errors.Is(err, context.Canceled) {
			t.Fatalf("New canceled = %v, want context.Canceled", err)
		}
	})
	t.Run("ContextDeadlineDuringDial", func(t *testing.T) {
		addr := unusedAddr(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if _, err := New(ctx, 1, 2, addr); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("New deadline = %v, want context deadline", err)
		}
	})
	t.Run("WrongPeerCountRejected", func(t *testing.T) {
		addr := unusedAddr(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		errc := make(chan error, 1)
		go func() {
			c, err := New(ctx, 0, 2, addr)
			if c != nil {
				_ = c.Close()
			}
			errc <- err
		}()
		conn, err := dialEventually(ctx, addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err := writeJSON(ctx, conn, hello{Rank: 1, Size: 3}); err != nil {
			t.Fatal(err)
		}
		if err := <-errc; err == nil {
			t.Fatal("rank zero accepted wrong peer count")
		}
	})
	t.Run("DuplicateRankRejected", func(t *testing.T) {
		addr := unusedAddr(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		errc := make(chan error, 1)
		go func() {
			c, err := New(ctx, 0, 3, addr)
			if c != nil {
				_ = c.Close()
			}
			errc <- err
		}()
		for i := 0; i < 2; i++ {
			conn, err := dialEventually(ctx, addr)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := writeJSON(ctx, conn, hello{Rank: 1, Size: 3}); err != nil {
				t.Fatal(err)
			}
			_, _ = readJSON[hello](ctx, conn)
		}
		if err := <-errc; err == nil {
			t.Fatal("rank zero accepted duplicate rank")
		}
	})
}

func TestAllGatherMetadata(t *testing.T) {
	t.Run("OneValuePerRankAndPreservesRankOrder", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c0, c1 := newPair(t, ctx)
		defer c0.Close()
		defer c1.Close()
		gotc := make(chan [][]byte, 2)
		errc := make(chan error, 2)
		go gather(ctx, c0, []byte("zero"), gotc, errc)
		go gather(ctx, c1, []byte("one"), gotc, errc)
		for i := 0; i < 2; i++ {
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
			got := <-gotc
			want := [][]byte{[]byte("zero"), []byte("one")}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("AllGather = %q, want %q", got, want)
			}
		}
	})
	t.Run("RejectsOversizedMessage", func(t *testing.T) {
		var buf bytes.Buffer
		if err := WriteFrame(&buf, make([]byte, maxFrameSize+1)); !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("WriteFrame oversized = %v, want ErrFrameTooLarge", err)
		}
	})
	t.Run("RejectsMalformedFrame", func(t *testing.T) {
		var buf bytes.Buffer
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], 4)
		buf.Write(hdr[:])
		if _, err := ReadFrame(&buf); !errors.Is(err, ErrMalformedFrame) {
			t.Fatalf("ReadFrame malformed = %v, want ErrMalformedFrame", err)
		}
	})
}

func TestBarrier(t *testing.T) {
	t.Run("AllRanksArrive", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c0, c1 := newPair(t, ctx)
		defer c0.Close()
		defer c1.Close()
		errc := make(chan error, 2)
		go func() { errc <- c0.Barrier(ctx) }()
		go func() { errc <- c1.Barrier(ctx) }()
		for i := 0; i < 2; i++ {
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("ContextCancellation", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		c := &Channel{rank: 1, size: 2, peers: []net.Conn{a, nil}}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if err := c.Barrier(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Barrier = %v, want context deadline", err)
		}
	})
	t.Run("PeerCloseUnblocksWaiters", func(t *testing.T) {
		a, b := net.Pipe()
		c := &Channel{rank: 1, size: 2, peers: []net.Conn{a, nil}}
		errc := make(chan error, 1)
		go func() { errc <- c.Barrier(context.Background()) }()
		_ = b.Close()
		if err := <-errc; err == nil {
			t.Fatal("Barrier after peer close = nil")
		}
		_ = a.Close()
	})
}

func TestClose(t *testing.T) {
	t.Run("Idempotent", func(t *testing.T) {
		c := &Channel{}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ClosesListenerAndConnections", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c0, c1 := newPair(t, ctx)
		if err := c0.Close(); err != nil {
			t.Fatal(err)
		}
		if err := c1.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("UnblocksPendingRead", func(t *testing.T) {
		a, _ := net.Pipe()
		c := &Channel{rank: 1, size: 2, peers: []net.Conn{a, nil}}
		errc := make(chan error, 1)
		go func() { errc <- c.Barrier(context.Background()) }()
		_ = c.Close()
		if err := <-errc; err == nil {
			t.Fatal("pending read was not interrupted")
		}
	})
}

func gather(ctx context.Context, c *Channel, data []byte, gotc chan<- [][]byte, errc chan<- error) {
	got, err := c.AllGather(ctx, data)
	errc <- err
	gotc <- got
}

func newPair(t *testing.T, ctx context.Context) (*Channel, *Channel) {
	t.Helper()
	addr := unusedAddr(t)
	type result struct {
		c   *Channel
		err error
	}
	r0 := make(chan result, 1)
	go func() {
		c, err := New(ctx, 0, 2, addr)
		r0 <- result{c, err}
	}()
	time.Sleep(10 * time.Millisecond)
	c1, err := New(ctx, 1, 2, addr)
	if err != nil {
		t.Fatal(err)
	}
	res := <-r0
	if res.err != nil {
		t.Fatal(res.err)
	}
	return res.c, c1
}

func unusedAddr(t *testing.T) string {
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

func dialEventually(ctx context.Context, addr string) (net.Conn, error) {
	var last error
	for {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			return conn, nil
		}
		last = err
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			if last != nil {
				return nil, last
			}
			return nil, ctx.Err()
		}
	}
}
