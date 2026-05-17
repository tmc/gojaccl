package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tmc/gojaccl/internal/jaccld/resource"
)

func TestSnapshotTitle(t *testing.T) {
	tests := []struct {
		name string
		s    snapshot
		want string
	}{
		{name: "error", s: snapshot{Err: errors.New("down")}, want: "J!"},
		{name: "ready empty", s: snapshot{Stats: resource.Stats{State: resource.StateReady}}, want: "J0"},
		{name: "not ready", s: snapshot{Stats: resource.Stats{State: resource.StateOpening}}, want: "J?"},
		{name: "live", s: snapshot{Stats: resource.Stats{State: resource.StateReady, Slots: resource.SlotStats{ProtectionDomains: resource.SlotCounter{Live: 1}, MemoryRegions: resource.SlotCounter{Live: 1}}}}, want: "J2"},
		{name: "outstanding", s: snapshot{Stats: resource.Stats{State: resource.StateReady, Slots: resource.SlotStats{QueuePairs: resource.SlotCounter{Outstanding: 3}}}}, want: "Jo3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.title(); got != tt.want {
				t.Fatalf("title() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSnapshotMenuLines(t *testing.T) {
	s := snapshot{
		Socket:    "/tmp/jaccld/run/jaccld.sock",
		CheckedAt: time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC),
		Stats: resource.Stats{
			State:  resource.StateReady,
			Leases: 2,
			MemoryRegions: resource.PoolStats{
				InUse:          1,
				Available:      3,
				BytesInUse:     4096,
				BytesAvailable: 8192,
			},
			QueuePairs:       resource.PoolStats{InUse: 1, Available: 1},
			CompletionQueues: resource.PoolStats{InUse: 1, Available: 1},
			Slots: resource.SlotStats{
				BootID:             "darwin-abcdef0123456789",
				Source:             "jaccld-observed",
				StatePath:          "/tmp/jaccld/slots/boot.json",
				ExternalUseUnknown: true,
				ProtectionDomains:  resource.SlotCounter{Opened: 1, Outstanding: 1, Live: 1},
				MemoryRegions:      resource.SlotCounter{Opened: 1, Outstanding: 1, Live: 1},
				QueuePairs:         resource.SlotCounter{Opened: 2, Closed: 1, Outstanding: 1, Live: 1},
			},
		},
	}
	got := strings.Join(s.menuLines(), "\n")
	for _, want := range []string{
		"Socket: /tmp/jaccld/run/jaccld.sock",
		"State: ready",
		"Leases: 2",
		"Memory pool: in_use=1 available=3 bytes=4096/8192",
		"Slot ledger: jaccld-observed",
		"External provider use: unknown",
		"protection_domain: live=1 outstanding=1 opened=1 closed=0 failed=0",
		"queue_pair: live=1 outstanding=1 opened=2 closed=1 failed=0",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("menuLines missing %q in:\n%s", want, got)
		}
	}
}

func TestTooltip(t *testing.T) {
	s := snapshot{
		Socket:    "/tmp/jaccld.sock",
		CheckedAt: time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC),
		Stats: resource.Stats{
			State: resource.StateReady,
			Slots: resource.SlotStats{
				ProtectionDomains: resource.SlotCounter{Live: 1, Failed: 1},
				MemoryRegions:     resource.SlotCounter{Outstanding: 2},
			},
		},
	}
	got := s.tooltip()
	for _, want := range []string{
		"jaccld",
		"/tmp/jaccld.sock",
		"state=ready leases=0",
		"live slots=1 outstanding=2 failed=1",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("tooltip missing %q in:\n%s", want, got)
		}
	}
}
