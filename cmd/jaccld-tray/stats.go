package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tmc/gojaccl/internal/ipc"
	"github.com/tmc/gojaccl/internal/jaccld/resource"
)

type snapshot struct {
	Socket    string
	CheckedAt time.Time
	Stats     resource.Stats
	Err       error
}

func fetchSnapshot(ctx context.Context, socket string) snapshot {
	s := snapshot{
		Socket:    socket,
		CheckedAt: time.Now(),
	}
	client, err := ipc.Dial(ctx, socket)
	if err != nil {
		s.Err = fmt.Errorf("dial jaccld: %w", err)
		return s
	}
	defer client.Close()
	stats, err := client.ResourceStats(ctx)
	if err != nil {
		s.Err = fmt.Errorf("resource stats: %w", err)
		return s
	}
	s.Stats = stats
	return s
}

func (s snapshot) title() string {
	if s.Err != nil {
		return "J!"
	}
	if live := s.liveSlots(); live > 0 {
		return fmt.Sprintf("J%d", live)
	}
	if outstanding := s.outstandingSlots(); outstanding > 0 {
		return fmt.Sprintf("Jo%d", outstanding)
	}
	if s.Stats.State != resource.StateReady {
		return "J?"
	}
	return "J0"
}

func (s snapshot) tooltip() string {
	lines := []string{"jaccld", s.Socket}
	if s.Err != nil {
		lines = append(lines, "error: "+s.Err.Error())
		return strings.Join(lines, "\n")
	}
	lines = append(lines,
		fmt.Sprintf("state=%s leases=%d", s.Stats.State, s.Stats.Leases),
		fmt.Sprintf("live slots=%d outstanding=%d failed=%d", s.liveSlots(), s.outstandingSlots(), s.failedSlots()),
	)
	if !s.CheckedAt.IsZero() {
		lines = append(lines, "checked "+s.CheckedAt.Format(time.Kitchen))
	}
	return strings.Join(lines, "\n")
}

func (s snapshot) menuLines() []string {
	lines := []string{
		"Socket: " + s.Socket,
	}
	if !s.CheckedAt.IsZero() {
		lines = append(lines, "Last check: "+s.CheckedAt.Format(time.RFC3339))
	}
	if s.Err != nil {
		return append(lines, "Error: "+s.Err.Error())
	}
	lines = append(lines,
		fmt.Sprintf("State: %s", s.Stats.State),
		fmt.Sprintf("Leases: %d", s.Stats.Leases),
		fmt.Sprintf("Memory pool: in_use=%d available=%d bytes=%d/%d",
			s.Stats.MemoryRegions.InUse,
			s.Stats.MemoryRegions.Available,
			s.Stats.MemoryRegions.BytesInUse,
			s.Stats.MemoryRegions.BytesAvailable,
		),
		fmt.Sprintf("QP pool: in_use=%d available=%d", s.Stats.QueuePairs.InUse, s.Stats.QueuePairs.Available),
		fmt.Sprintf("CQ pool: in_use=%d available=%d", s.Stats.CompletionQueues.InUse, s.Stats.CompletionQueues.Available),
	)
	slots := s.Stats.Slots
	if slots.BootID != "" {
		lines = append(lines,
			"Slot ledger: "+slots.Source,
			"Boot: "+short(slots.BootID, 24),
		)
		if slots.ExternalUseUnknown {
			lines = append(lines, "External provider use: unknown")
		}
		if slots.StatePath != "" {
			lines = append(lines, "State file: "+slots.StatePath)
		}
	}
	for _, line := range slotLines(slots) {
		lines = append(lines, line)
	}
	return lines
}

func (s snapshot) liveSlots() int {
	return s.Stats.Slots.ProtectionDomains.Live +
		s.Stats.Slots.MemoryRegions.Live +
		s.Stats.Slots.QueuePairs.Live +
		s.Stats.Slots.CompletionQueues.Live
}

func (s snapshot) outstandingSlots() uint64 {
	return s.Stats.Slots.ProtectionDomains.Outstanding +
		s.Stats.Slots.MemoryRegions.Outstanding +
		s.Stats.Slots.QueuePairs.Outstanding +
		s.Stats.Slots.CompletionQueues.Outstanding
}

func (s snapshot) failedSlots() uint64 {
	return s.Stats.Slots.ProtectionDomains.Failed +
		s.Stats.Slots.MemoryRegions.Failed +
		s.Stats.Slots.QueuePairs.Failed +
		s.Stats.Slots.CompletionQueues.Failed
}

func slotLines(slots resource.SlotStats) []string {
	return []string{
		slotLine(resource.SlotProtectionDomain, slots.ProtectionDomains),
		slotLine(resource.SlotMemoryRegion, slots.MemoryRegions),
		slotLine(resource.SlotQueuePair, slots.QueuePairs),
		slotLine(resource.SlotCompletionQueue, slots.CompletionQueues),
	}
}

func slotLine(kind resource.SlotKind, c resource.SlotCounter) string {
	return fmt.Sprintf("%s: live=%d outstanding=%d opened=%d closed=%d failed=%d",
		kind, c.Live, c.Outstanding, c.Opened, c.Closed, c.Failed)
}

func short(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n]
}
