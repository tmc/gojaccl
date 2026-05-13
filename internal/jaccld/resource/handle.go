package resource

import (
	"context"
	"fmt"
	"sync"
)

// StaticQueuePairPool leases pre-created queue-pair handles.
type StaticQueuePairPool struct {
	mu        sync.Mutex
	available []QueuePairHandle
	inUse     map[QueuePairHandle]PeerSpec
}

// NewStaticQueuePairPool returns a queue-pair pool backed by handles.
func NewStaticQueuePairPool(handles []QueuePairHandle) (*StaticQueuePairPool, error) {
	if len(handles) == 0 {
		return nil, fmt.Errorf("new queue-pair pool: no handles")
	}
	seen := make(map[QueuePairHandle]bool, len(handles))
	for _, h := range handles {
		if h == "" {
			return nil, fmt.Errorf("new queue-pair pool: empty handle")
		}
		if seen[h] {
			return nil, fmt.Errorf("new queue-pair pool: duplicate handle %q", h)
		}
		seen[h] = true
	}
	return &StaticQueuePairPool{
		available: append([]QueuePairHandle(nil), handles...),
		inUse:     make(map[QueuePairHandle]PeerSpec),
	}, nil
}

// AcquireQueuePair leases one queue-pair handle.
func (p *StaticQueuePairPool) AcquireQueuePair(ctx context.Context, peer PeerSpec) (QueuePairHandle, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.available) == 0 {
		return "", ErrExhausted
	}
	h := p.available[len(p.available)-1]
	p.available = p.available[:len(p.available)-1]
	p.inUse[h] = peer
	return h, nil
}

// ReleaseQueuePair releases one queue-pair handle.
func (p *StaticQueuePairPool) ReleaseQueuePair(ctx context.Context, h QueuePairHandle) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.inUse[h]; !ok {
		return ErrLeaseNotFound
	}
	delete(p.inUse, h)
	p.available = append(p.available, h)
	return nil
}

// QueuePairStats reports queue-pair pool use.
func (p *StaticQueuePairPool) QueuePairStats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{InUse: len(p.inUse), Available: len(p.available)}
}

// StaticCompletionQueuePool leases pre-created completion-queue handles.
type StaticCompletionQueuePool struct {
	mu        sync.Mutex
	available []CompletionQueueHandle
	inUse     map[CompletionQueueHandle]bool
}

// NewStaticCompletionQueuePool returns a completion-queue pool backed by handles.
func NewStaticCompletionQueuePool(handles []CompletionQueueHandle) (*StaticCompletionQueuePool, error) {
	if len(handles) == 0 {
		return nil, fmt.Errorf("new completion-queue pool: no handles")
	}
	seen := make(map[CompletionQueueHandle]bool, len(handles))
	for _, h := range handles {
		if h == "" {
			return nil, fmt.Errorf("new completion-queue pool: empty handle")
		}
		if seen[h] {
			return nil, fmt.Errorf("new completion-queue pool: duplicate handle %q", h)
		}
		seen[h] = true
	}
	return &StaticCompletionQueuePool{
		available: append([]CompletionQueueHandle(nil), handles...),
		inUse:     make(map[CompletionQueueHandle]bool),
	}, nil
}

// AcquireCompletionQueue leases one completion-queue handle.
func (p *StaticCompletionQueuePool) AcquireCompletionQueue(ctx context.Context) (CompletionQueueHandle, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.available) == 0 {
		return "", ErrExhausted
	}
	h := p.available[len(p.available)-1]
	p.available = p.available[:len(p.available)-1]
	p.inUse[h] = true
	return h, nil
}

// ReleaseCompletionQueue releases one completion-queue handle.
func (p *StaticCompletionQueuePool) ReleaseCompletionQueue(ctx context.Context, h CompletionQueueHandle) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.inUse[h] {
		return ErrLeaseNotFound
	}
	delete(p.inUse, h)
	p.available = append(p.available, h)
	return nil
}

// CompletionQueueStats reports completion-queue pool use.
func (p *StaticCompletionQueuePool) CompletionQueueStats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{InUse: len(p.inUse), Available: len(p.available)}
}
