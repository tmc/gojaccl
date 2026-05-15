//go:build !darwin || !arm64

package rdma

import (
	"context"
	"fmt"
)

func Available() bool {
	return false
}

func DeviceNames() ([]string, error) {
	return nil, ErrUnavailable
}

func OpenDevice(path string) (*Device, error) {
	return nil, fmt.Errorf("open device %q: %w", path, ErrUnavailable)
}

func NewProtectionDomain(dev *Device) (*ProtectionDomain, error) {
	return nil, ErrUnavailable
}

func NewCompletionQueue(dev *Device, capacity int) (*CompletionQueue, error) {
	return nil, ErrUnavailable
}

func NewQueuePair(pd *ProtectionDomain, cq *CompletionQueue) (*QueuePair, error) {
	return nil, ErrUnavailable
}

func LocalDestination(qp *QueuePair) (Destination, error) {
	return Destination{}, ErrUnavailable
}

func QueryPort(dev *Device) (PortInfo, error) {
	return PortInfo{}, ErrUnavailable
}

func InitQueuePair(qp *QueuePair) error {
	return ErrUnavailable
}

func ReadyToReceive(qp *QueuePair, dst Destination) error {
	return ErrUnavailable
}

func ReadyToSend(qp *QueuePair, psn uint32) error {
	return ErrUnavailable
}

func RegisterMemory(pd *ProtectionDomain, buf []byte) (*MemoryRegion, error) {
	return nil, ErrUnavailable
}

func NewMemoryRegion(pd *ProtectionDomain, size int) (*MemoryRegion, error) {
	return nil, ErrUnavailable
}

func PostSend(qp *QueuePair, mr *MemoryRegion, offset, length int, id uint64) error {
	return ErrUnavailable
}

func PostRecv(qp *QueuePair, mr *MemoryRegion, offset, length int, id uint64) error {
	return ErrUnavailable
}

func PostWrite(qp *QueuePair, mr *MemoryRegion, offset, length int, remoteAddr uint64, rkey uint32, id uint64) error {
	return ErrUnavailable
}

func PollCompletion(ctx context.Context, cq *CompletionQueue) ([]WorkRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, ErrUnavailable
}

func (d *Device) Close() error { return nil }

func (p *ProtectionDomain) Close() error { return nil }

func (c *CompletionQueue) Close() error { return nil }

func (q *QueuePair) Close() error { return nil }

func (q *QueuePair) number() uint32 { return 0 }

func (m *MemoryRegion) Close() error { return nil }
