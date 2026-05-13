// Package ipc implements the jaccld Unix-domain socket control protocol.
package ipc

import (
	"fmt"

	"github.com/tmc/gojaccl/internal/allocator"
)

// DefaultSocket is the default jaccld Unix-domain socket path.
const DefaultSocket = "/tmp/jaccld.sock"

const (
	opAlloc = "alloc"
	opFree  = "free"
	opMap   = "map"
	opStats = "stats"
)

// Request is one client control request.
type Request struct {
	Op      string `json:"op"`
	Size    int64  `json:"size,omitempty"`
	LeaseID uint64 `json:"lease_id,omitempty"`
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
