package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tmc/gojaccl/internal/reduce"
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
