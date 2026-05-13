package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/tmc/gojaccl/internal/ipc"
)

func TestConfigValidateRDMA(t *testing.T) {
	tests := []struct {
		name string
		cfg  config
		want string
	}{
		{
			name: "valid",
			cfg:  config{rank: 0, size: 2, coordinator: "127.0.0.1:12345", heartbeat: time.Second},
		},
		{
			name: "negative rank",
			cfg:  config{rank: -1, size: 2, coordinator: "127.0.0.1:12345", heartbeat: time.Second},
			want: "rank -1 out of range",
		},
		{
			name: "zero size",
			cfg:  config{rank: 0, size: 0, coordinator: "127.0.0.1:12345", heartbeat: time.Second},
			want: "size 0 must be positive",
		},
		{
			name: "rank out of range",
			cfg:  config{rank: 2, size: 2, coordinator: "127.0.0.1:12345", heartbeat: time.Second},
			want: "rank 2 out of range for size 2",
		},
		{
			name: "empty coordinator",
			cfg:  config{rank: 0, size: 2, heartbeat: time.Second},
			want: "coordinator is empty",
		},
		{
			name: "zero heartbeat",
			cfg:  config{rank: 0, size: 2, coordinator: "127.0.0.1:12345"},
			want: "heartbeat interval 0s must be positive",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validateRDMA()
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validateRDMA = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validateRDMA = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRunNoRDMAStartsIPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	socket := t.TempDir() + "/jaccld.sock"
	errc := make(chan error, 1)
	go func() {
		errc <- run(ctx, config{
			socket:      socket,
			slabSize:    4096,
			maxSessions: 2,
			noRDMA:      true,
		})
	}()

	client := dialUntilReady(t, socket)
	defer client.Close()

	stats, err := client.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Size != 4096 {
		t.Fatalf("stats size = %d, want 4096", stats.Size)
	}
	resourceStats, err := client.ResourceStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if resourceStats.Leases != 0 || resourceStats.QueuePairs.Available != 2 || resourceStats.CompletionQueues.Available != 2 {
		t.Fatalf("resource stats = %+v, want empty store with two handles", resourceStats)
	}
	if err := client.Barrier(context.Background()); err == nil || !strings.Contains(err.Error(), ipc.ErrNoTransport.Error()) {
		t.Fatalf("Barrier = %v, want %q", err, ipc.ErrNoTransport)
	}

	cancel()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("run = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not stop after cancel")
	}
}

func dialUntilReady(t *testing.T, socket string) *ipc.Client {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	var last error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		client, err := ipc.Dial(ctx, socket)
		cancel()
		if err == nil {
			return client
		}
		last = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("dial jaccld: %v", last)
	return nil
}
