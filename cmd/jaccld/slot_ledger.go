package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tmc/gojaccl/internal/jaccld/resource"
)

const defaultSlotStateDir = "/tmp/jaccld/slots"

type slotLedger struct {
	mu    sync.Mutex
	path  string
	stats resource.SlotStats
	file  slotLedgerFile
}

type slotLedgerFile struct {
	BootID            string               `json:"boot_id"`
	UpdatedAt         time.Time            `json:"updated_at"`
	ProtectionDomains slotLedgerFileCounts `json:"protection_domains"`
	MemoryRegions     slotLedgerFileCounts `json:"memory_regions"`
	QueuePairs        slotLedgerFileCounts `json:"queue_pairs"`
	CompletionQueues  slotLedgerFileCounts `json:"completion_queues"`
}

type slotLedgerFileCounts struct {
	Opened uint64 `json:"opened"`
	Closed uint64 `json:"closed"`
	Failed uint64 `json:"failed"`
}

func newSlotLedger(dir string) (*slotLedger, error) {
	bootID, err := currentBootID()
	if err != nil {
		return nil, fmt.Errorf("slot ledger boot id: %w", err)
	}
	return newSlotLedgerWithBootID(dir, bootID)
}

func newSlotLedgerWithBootID(dir, bootID string) (*slotLedger, error) {
	if strings.TrimSpace(bootID) == "" {
		return nil, fmt.Errorf("slot ledger: empty boot id")
	}
	if dir == "" {
		dir = defaultSlotStateDir
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("slot ledger: create state directory: %w", err)
	}
	path := filepath.Join(dir, safeBootID(bootID)+".json")
	l := &slotLedger{path: path}
	l.file = slotLedgerFile{BootID: bootID}
	data, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(data, &l.file); err != nil {
			return nil, fmt.Errorf("slot ledger: read %s: %w", path, err)
		}
		if l.file.BootID != bootID {
			l.file = slotLedgerFile{BootID: bootID}
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("slot ledger: read %s: %w", path, err)
	}
	l.refreshStatsLocked()
	if err := l.saveLocked(); err != nil {
		return nil, err
	}
	return l, nil
}

func safeBootID(id string) string {
	var b strings.Builder
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func (l *slotLedger) MarkOpened(kind resource.SlotKind) (resource.SlotStats, error) {
	return l.update(kind, func(c *slotLedgerFileCounts, live *int) {
		c.Opened++
		*live = *live + 1
	})
}

func (l *slotLedger) MarkClosed(kind resource.SlotKind) (resource.SlotStats, error) {
	return l.update(kind, func(c *slotLedgerFileCounts, live *int) {
		c.Closed++
		if *live > 0 {
			*live--
		}
	})
}

func (l *slotLedger) MarkFailed(kind resource.SlotKind) (resource.SlotStats, error) {
	return l.update(kind, func(c *slotLedgerFileCounts, live *int) {
		c.Failed++
	})
}

func (l *slotLedger) Stats() resource.SlotStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stats
}

func (l *slotLedger) update(kind resource.SlotKind, fn func(*slotLedgerFileCounts, *int)) (resource.SlotStats, error) {
	if l == nil {
		return resource.SlotStats{}, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	counts, live, err := l.counterLocked(kind)
	if err != nil {
		return l.stats, err
	}
	fn(counts, live)
	l.refreshStatsLocked()
	if err := l.saveLocked(); err != nil {
		return l.stats, err
	}
	return l.stats, nil
}

func (l *slotLedger) counterLocked(kind resource.SlotKind) (*slotLedgerFileCounts, *int, error) {
	switch kind {
	case resource.SlotProtectionDomain:
		return &l.file.ProtectionDomains, &l.stats.ProtectionDomains.Live, nil
	case resource.SlotMemoryRegion:
		return &l.file.MemoryRegions, &l.stats.MemoryRegions.Live, nil
	case resource.SlotQueuePair:
		return &l.file.QueuePairs, &l.stats.QueuePairs.Live, nil
	case resource.SlotCompletionQueue:
		return &l.file.CompletionQueues, &l.stats.CompletionQueues.Live, nil
	default:
		return nil, nil, fmt.Errorf("slot ledger: unknown slot kind %q", kind)
	}
}

func (l *slotLedger) refreshStatsLocked() {
	l.stats.BootID = l.file.BootID
	l.stats.Source = "jaccld-observed"
	l.stats.StatePath = l.path
	l.stats.ExternalUseUnknown = true
	l.stats.ProtectionDomains = slotCounter(l.file.ProtectionDomains, l.stats.ProtectionDomains.Live)
	l.stats.MemoryRegions = slotCounter(l.file.MemoryRegions, l.stats.MemoryRegions.Live)
	l.stats.QueuePairs = slotCounter(l.file.QueuePairs, l.stats.QueuePairs.Live)
	l.stats.CompletionQueues = slotCounter(l.file.CompletionQueues, l.stats.CompletionQueues.Live)
}

func slotCounter(c slotLedgerFileCounts, live int) resource.SlotCounter {
	outstanding := uint64(0)
	if c.Opened >= c.Closed {
		outstanding = c.Opened - c.Closed
	}
	return resource.SlotCounter{
		Opened:      c.Opened,
		Closed:      c.Closed,
		Failed:      c.Failed,
		Outstanding: outstanding,
		Live:        live,
	}
}

func (l *slotLedger) saveLocked() error {
	l.file.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(l.file, "", "  ")
	if err != nil {
		return fmt.Errorf("slot ledger: encode: %w", err)
	}
	data = append(data, '\n')
	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return fmt.Errorf("slot ledger: write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, l.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("slot ledger: commit %s: %w", l.path, err)
	}
	return nil
}
