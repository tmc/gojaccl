package rdma

import (
	"errors"
	"sync"
	"unsafe"
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

// Addr reports the local address of the registered memory.
func (m *MemoryRegion) Addr() uint64 {
	if m == nil || len(m.buf) == 0 {
		return 0
	}
	return uint64(uintptr(unsafe.Pointer(&m.buf[0])))
}

// Destination is the queue-pair metadata exchanged on the TCP side channel.
// GIDIndex is the local provider GID-table index used for this destination.
type Destination struct {
	LID      uint16
	QPN      uint32
	PSN      uint32
	GIDIndex int
	GID      [16]byte
}

// PortInfo is the provider metadata needed before an RTR transition.
type PortInfo struct {
	Device           string
	PortNum          int
	LID              uint16
	GIDTableLength   int
	GIDScanLimit     int
	SelectedGIDIndex int
	GIDs             []GIDEntry
}

// GIDEntry is one entry from a provider GID table.
type GIDEntry struct {
	Index      int
	GID        [16]byte
	IPv4Mapped bool
	Zero       bool
}

// WorkRequest describes a completed work request.
type WorkRequest struct {
	ID     uint64
	Opcode int
	Bytes  int
	Status int
}
