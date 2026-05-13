// Package ipc implements the jaccld Unix-domain socket control protocol.
package ipc

import (
	"errors"
	"fmt"

	"github.com/tmc/gojaccl/internal/allocator"
)

// ErrNoTransport reports that jaccld has no data transport configured.
var ErrNoTransport = errors.New("jaccld transport unavailable")

// DefaultSocket is the default jaccld Unix-domain socket path.
const DefaultSocket = "/tmp/jaccld.sock"

const (
	opAlloc   = "alloc"
	opFree    = "free"
	opMap     = "map"
	opStats   = "stats"
	opSend    = "send"
	opRecv    = "recv"
	opBarrier = "barrier"
)

// Request is one client control request.
type Request struct {
	Op string `json:"op"`
	// Size is the requested allocation size for alloc.
	Size int64 `json:"size,omitempty"`
	// LeaseID identifies the leased slab range for free, send, and recv.
	LeaseID uint64 `json:"lease_id,omitempty"`
	// Peer is the destination for send or source for recv.
	Peer int `json:"peer,omitempty"`
	// Offset and Length identify the byte range within a lease for send and recv.
	Offset int64 `json:"offset,omitempty"`
	Length int64 `json:"length,omitempty"`
}

// Response is one daemon control response.
type Response struct {
	OK       bool            `json:"ok"`
	Error    string          `json:"error,omitempty"`
	Lease    allocator.Lease `json:"lease,omitempty"`
	Stats    allocator.Stats `json:"stats,omitempty"`
	SlabSize int64           `json:"slab_size,omitempty"`
}

func (r Response) err() error {
	if r.OK {
		return nil
	}
	if r.Error == "" {
		return fmt.Errorf("jaccld request failed")
	}
	return fmt.Errorf("%s", r.Error)
}
