package rdma

import (
	"errors"
	"sync"
)

// ErrUnavailable reports that the platform RDMA backend cannot be used.
var ErrUnavailable = errors.New("rdma unavailable")

// Device is an opened RDMA device context.
type Device struct {
	handle uintptr
	name   string
	once   sync.Once
}

// Name reports the device name used to open the context.
func (d *Device) Name() string {
	if d == nil {
		return ""
	}
	return d.name
}

// ProtectionDomain is an RDMA protection domain.
type ProtectionDomain struct {
	dev    *Device
	handle uintptr
	once   sync.Once
}

// CompletionQueue is an RDMA completion queue.
type CompletionQueue struct {
	dev    *Device
	handle uintptr
	once   sync.Once
}

// QueuePair is a UC queue pair.
type QueuePair struct {
	pd     *ProtectionDomain
	cq     *CompletionQueue
	handle uintptr
	poster any
	once   sync.Once
}

// Number reports the backend queue-pair number.
func (q *QueuePair) Number() uint32 {
	if q == nil {
		return 0
	}
	return q.number()
}

// MemoryRegion is registered staging memory.
type MemoryRegion struct {
	pd     *ProtectionDomain
	handle uintptr
	buf    []byte
	lkey   uint32
	rkey   uint32
	mapped bool
	once   sync.Once
}

// Buffer reports the registered staging buffer.
func (m *MemoryRegion) Buffer() []byte {
	if m == nil {
		return nil
	}
	return m.buf
}

// LKey reports the local key used in work requests.
func (m *MemoryRegion) LKey() uint32 {
	if m == nil {
		return 0
	}
	return m.lkey
}

// RKey reports the remote key published to peers.
func (m *MemoryRegion) RKey() uint32 {
	if m == nil {
		return 0
	}
	return m.rkey
}

// Destination is the queue-pair metadata exchanged on the TCP side channel.
type Destination struct {
	LID      uint16
	QPN      uint32
	PSN      uint32
	GIDIndex int
	GID      [16]byte
}

// WorkRequest describes a completed work request.
type WorkRequest struct {
	ID     uint64
	Opcode int
	Bytes  int
	Status int
}
