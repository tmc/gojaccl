package jaccl

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/gojaccl/internal/reduce"
)

func TestNewGroup(t *testing.T) {
	t.Run("InvalidConfigBeforeBackend", func(t *testing.T) {
		called := false
		old := backendFactory
		backendFactory = func(context.Context, Config) (backend, error) {
			called = true
			return nil, nil
		}
		t.Cleanup(func() { backendFactory = old })
		cfg := fakeConfig(0, 2)
		cfg.Devices = nil
		if _, err := NewGroup(context.Background(), cfg); err == nil {
			t.Fatal("NewGroup invalid config = nil")
		}
		if called {
			t.Fatal("backend called for invalid config")
		}
	})
	t.Run("ContextCanceledBeforeStart", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := NewGroup(ctx, fakeConfig(0, 2)); !errors.Is(err, context.Canceled) {
			t.Fatalf("NewGroup canceled = %v, want context.Canceled", err)
		}
	})
	t.Run("ContextDeadlineDuringSideChannel", func(t *testing.T) {
		old := backendFactory
		backendFactory = func(ctx context.Context, _ Config) (backend, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		t.Cleanup(func() { backendFactory = old })
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if _, err := NewGroup(ctx, fakeConfig(0, 2)); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("NewGroup deadline = %v, want context deadline", err)
		}
	})
	t.Run("FakeBackendInitializes", func(t *testing.T) {
		useFakeBackend(t, newFakeNetwork(2))
		g, err := NewGroup(context.Background(), fakeConfig(0, 2))
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
	})
	t.Run("ReadyBeforeReturn", func(t *testing.T) {
		useFakeBackend(t, newFakeNetwork(2))
		g, err := NewGroup(context.Background(), fakeConfig(1, 2))
		if err != nil {
			t.Fatal(err)
		}
		if g.Rank() != 1 || g.Size() != 2 {
			t.Fatalf("group rank/size = %d/%d", g.Rank(), g.Size())
		}
	})
	t.Run("ErrorIncludesOperationAndRank", func(t *testing.T) {
		old := backendFactory
		backendFactory = func(context.Context, Config) (backend, error) {
			return nil, errors.New("boom")
		}
		t.Cleanup(func() { backendFactory = old })
		_, err := NewGroup(context.Background(), fakeConfig(1, 2))
		if err == nil || !strings.Contains(err.Error(), "rank 1 new group") {
			t.Fatalf("NewGroup error = %v, want operation and rank", err)
		}
	})
}

func TestNewGroupFromEnv(t *testing.T) {
	t.Run("UsesConfigFromEnv", func(t *testing.T) {
		useFakeBackend(t, newFakeNetwork(2))
		t.Setenv("JACCL_RANK", "0")
		t.Setenv("JACCL_COORDINATOR", "host:1")
		t.Setenv("JACCL_IBV_DEVICES", writeDevices(t, fakeConfig(0, 2).Devices))
		g, err := NewGroupFromEnv(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
	})
	t.Run("PropagatesContext", func(t *testing.T) {
		t.Setenv("JACCL_RANK", "0")
		t.Setenv("JACCL_COORDINATOR", "host:1")
		t.Setenv("JACCL_IBV_DEVICES", writeDevices(t, fakeConfig(0, 2).Devices))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := NewGroupFromEnv(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("NewGroupFromEnv canceled = %v", err)
		}
	})
	t.Run("WrapsConfigError", func(t *testing.T) {
		if _, err := NewGroupFromEnv(context.Background()); err == nil || !strings.Contains(err.Error(), "new group from env") {
			t.Fatalf("NewGroupFromEnv = %v, want config wrapper", err)
		}
	})
}

func TestGroupRankSize(t *testing.T) {
	t.Run("RankZero", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		if g.Rank() != 0 || g.Size() != 2 {
			t.Fatalf("rank/size = %d/%d", g.Rank(), g.Size())
		}
	})
	t.Run("RankOneOfTwo", func(t *testing.T) {
		g := newFakeGroup(1, 2, newFakeNetwork(2))
		if g.Rank() != 1 || g.Size() != 2 {
			t.Fatalf("rank/size = %d/%d", g.Rank(), g.Size())
		}
	})
}

func TestGroupClose(t *testing.T) {
	t.Run("Idempotent", func(t *testing.T) {
		net := newFakeNetwork(2)
		g := newFakeGroup(0, 2, net)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if net.closed != 1 {
			t.Fatalf("backend close count = %d, want 1", net.closed)
		}
	})
	t.Run("ReleasesResourcesInReverseOrder", func(t *testing.T) {
		net := newFakeNetwork(2)
		g := newFakeGroup(0, 2, net)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if net.closed != 1 {
			t.Fatalf("backend close count = %d, want 1", net.closed)
		}
	})
	t.Run("ConcurrentClose", func(t *testing.T) {
		net := newFakeNetwork(2)
		g := newFakeGroup(0, 2, net)
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = g.Close()
			}()
		}
		wg.Wait()
		if net.closed != 1 {
			t.Fatalf("backend close count = %d, want 1", net.closed)
		}
	})
	t.Run("OperationAfterClose", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		_ = g.Close()
		if err := g.Barrier(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("Barrier after close = %v, want ErrClosed", err)
		}
	})
}

func TestOperationSerialization(t *testing.T) {
	t.Run("SecondOperationRunsAfterFirstCompletes", func(t *testing.T) {
		b := newBlockingBackend()
		g := newBlockingGroup(b)
		first := make(chan error, 1)
		go func() { first <- g.Barrier(context.Background()) }()
		<-b.started
		second := make(chan error, 1)
		go func() { second <- g.Barrier(context.Background()) }()
		select {
		case err := <-second:
			t.Fatalf("second operation returned early: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
		close(b.release)
		if err := <-first; err != nil {
			t.Fatal(err)
		}
		if err := <-second; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("SecondOperationWaitsUntilContextDone", func(t *testing.T) {
		b := newBlockingBackend()
		g := newBlockingGroup(b)
		go func() { _ = g.Barrier(context.Background()) }()
		<-b.started
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if err := g.Barrier(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second operation = %v, want deadline", err)
		}
		close(b.release)
	})
	t.Run("InvalidOperationDoesNotTouchBackend", func(t *testing.T) {
		b := newBlockingBackend()
		g := newBlockingGroup(b)
		if err := g.Send(context.Background(), 4, nil); err == nil {
			t.Fatal("invalid send = nil")
		}
		select {
		case <-b.started:
			t.Fatal("backend touched for invalid operation")
		default:
		}
	})
	t.Run("CloseDuringOperationCancelsWaiters", func(t *testing.T) {
		b := newBlockingBackend()
		g := newBlockingGroup(b)
		go func() { _ = g.Barrier(context.Background()) }()
		<-b.started
		done := make(chan error, 1)
		go func() { done <- g.Barrier(context.Background()) }()
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; !errors.Is(err, ErrClosed) {
			t.Fatalf("waiting operation = %v, want ErrClosed", err)
		}
		close(b.release)
	})
}

type blockingBackend struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingBackend() *blockingBackend {
	return &blockingBackend{started: make(chan struct{}), release: make(chan struct{})}
}

func newBlockingGroup(b backend) *Group {
	return &Group{rank: 0, size: 2, b: b, op: make(chan struct{}, 1), closed: make(chan struct{})}
}

func (b *blockingBackend) barrier(ctx context.Context) error {
	b.once.Do(func() { close(b.started) })
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blockingBackend) send(context.Context, int, []byte) error { return nil }
func (b *blockingBackend) recv(context.Context, int, []byte) error { return nil }
func (b *blockingBackend) allReduce(context.Context, reductionOp, reduce.DType, []byte, []byte) error {
	return nil
}
func (b *blockingBackend) allGather(context.Context, int, []byte, []byte) error { return nil }
func (b *blockingBackend) close() error                                         { return nil }
