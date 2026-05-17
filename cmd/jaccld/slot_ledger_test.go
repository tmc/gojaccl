package main

import (
	"testing"

	"github.com/tmc/gojaccl/internal/jaccld/resource"
)

func TestSlotLedgerPersistsPerBoot(t *testing.T) {
	dir := t.TempDir()
	ledger, err := newSlotLedgerWithBootID(dir, "boot:one")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkOpened(resource.SlotProtectionDomain); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkOpened(resource.SlotMemoryRegion); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkClosed(resource.SlotMemoryRegion); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkFailed(resource.SlotQueuePair); err != nil {
		t.Fatal(err)
	}
	stats := ledger.Stats()
	if stats.ProtectionDomains.Opened != 1 || stats.ProtectionDomains.Outstanding != 1 || stats.ProtectionDomains.Live != 1 {
		t.Fatalf("pd stats = %+v, want one opened/outstanding/live", stats.ProtectionDomains)
	}
	if stats.MemoryRegions.Opened != 1 || stats.MemoryRegions.Closed != 1 || stats.MemoryRegions.Outstanding != 0 || stats.MemoryRegions.Live != 0 {
		t.Fatalf("mr stats = %+v, want one opened and closed", stats.MemoryRegions)
	}
	if stats.QueuePairs.Failed != 1 {
		t.Fatalf("qp failed = %d, want 1", stats.QueuePairs.Failed)
	}

	restarted, err := newSlotLedgerWithBootID(dir, "boot:one")
	if err != nil {
		t.Fatal(err)
	}
	stats = restarted.Stats()
	if stats.ProtectionDomains.Opened != 1 || stats.ProtectionDomains.Outstanding != 1 {
		t.Fatalf("restarted pd stats = %+v, want persisted opened/outstanding", stats.ProtectionDomains)
	}
	if stats.ProtectionDomains.Live != 0 {
		t.Fatalf("restarted pd live = %d, want 0 for new process", stats.ProtectionDomains.Live)
	}
	if stats.StatePath == "" || stats.BootID != "boot:one" || !stats.ExternalUseUnknown {
		t.Fatalf("restarted slot metadata = %+v, want path boot and external unknown", stats)
	}

	nextBoot, err := newSlotLedgerWithBootID(dir, "boot:two")
	if err != nil {
		t.Fatal(err)
	}
	if stats := nextBoot.Stats(); stats.ProtectionDomains.Opened != 0 || stats.MemoryRegions.Opened != 0 || stats.QueuePairs.Failed != 0 {
		t.Fatalf("next boot stats = %+v, want reset counters", stats)
	}
}

func TestSafeBootID(t *testing.T) {
	if got, want := safeBootID("darwin:1/2"), "darwin_1_2"; got != want {
		t.Fatalf("safeBootID = %q, want %q", got, want)
	}
}
