package resource

import (
	"errors"
	"fmt"
	"time"
)

// Errors reported by the resource lease model.
var (
	ErrNotReady       = errors.New("jaccld resource store not ready")
	ErrExhausted      = errors.New("jaccld resource exhausted")
	ErrInvalidRequest = errors.New("invalid resource request")
	ErrInvalidState   = errors.New("invalid daemon state")
	ErrLeaseNotFound  = errors.New("resource lease not found")
	ErrExpired        = errors.New("resource lease expired")
)

// State is jaccld's resource admission state.
type State uint8

const (
	StateBootstrapping State = iota
	StateOpening
	StateReady
	StateDraining
	StateTerminated
)

func (s State) valid() bool {
	return s <= StateTerminated
}

func (s State) String() string {
	switch s {
	case StateBootstrapping:
		return "bootstrapping"
	case StateOpening:
		return "opening"
	case StateReady:
		return "ready"
	case StateDraining:
		return "draining"
	case StateTerminated:
		return "terminated"
	default:
		return fmt.Sprintf("state(%d)", s)
	}
}

// LeaseID identifies a daemon-issued session lease.
type LeaseID uint64

// QueuePairHandle identifies a queue pair in a daemon-owned pool.
type QueuePairHandle string

// CompletionQueueHandle identifies a completion queue in a daemon-owned pool.
type CompletionQueueHandle string

// RemoteMemory identifies a peer's registered memory region.
type RemoteMemory struct {
	Addr uint64 `json:"addr"`
	RKey uint32 `json:"rkey"`
}

// PeerSpec describes a formed peer route without importing a provider package.
type PeerSpec struct {
	Rank                 int          `json:"rank"`
	QueuePairNumber      uint32       `json:"queue_pair_number,omitempty"`
	PacketSequenceNumber uint32       `json:"packet_sequence_number,omitempty"`
	LocalID              uint16       `json:"local_id,omitempty"`
	GlobalID             string       `json:"global_id,omitempty"`
	MTU                  int          `json:"mtu,omitempty"`
	Memory               RemoteMemory `json:"memory,omitempty"`
}

// MRWindow is a logical window in a pre-registered memory region.
type MRWindow struct {
	SlabID string `json:"slab_id,omitempty"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

// SessionRequest asks jaccld for bounded resources for one client session.
type SessionRequest struct {
	ClientID string    `json:"client_id"`
	Peer     PeerSpec  `json:"peer"`
	Size     int64     `json:"size"`
	Deadline time.Time `json:"deadline"`
	// Heartbeat is an optional requested idle interval. Zero means the
	// daemon default; negative values are invalid.
	Heartbeat time.Duration `json:"heartbeat,omitempty"`
}

// SessionLease is a daemon-issued lease over pre-created resources.
type SessionLease struct {
	ID              LeaseID               `json:"id"`
	ClientID        string                `json:"client_id"`
	Peer            PeerSpec              `json:"peer"`
	Window          MRWindow              `json:"window"`
	QueuePair       QueuePairHandle       `json:"queue_pair"`
	CompletionQueue CompletionQueueHandle `json:"completion_queue"`
	CreatedAt       time.Time             `json:"created_at"`
	ExpiresAt       time.Time             `json:"expires_at"`
}

// Stats reports resource store and pool use.
type Stats struct {
	State            State
	Leases           int
	MemoryRegions    PoolStats
	QueuePairs       PoolStats
	CompletionQueues PoolStats
}
