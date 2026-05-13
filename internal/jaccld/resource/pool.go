package resource

import "context"

// PoolStats reports current use of a bounded daemon resource pool.
type PoolStats struct {
	InUse          int
	Available      int
	BytesInUse     int64
	BytesAvailable int64
}

// MRPool leases windows from a pre-registered memory region.
type MRPool interface {
	AllocMR(context.Context, int64) (MRWindow, error)
	FreeMR(context.Context, MRWindow) error
	MRStats() PoolStats
}

// QueuePairPool leases queue-pair handles created during daemon startup.
type QueuePairPool interface {
	AcquireQueuePair(context.Context, PeerSpec) (QueuePairHandle, error)
	ReleaseQueuePair(context.Context, QueuePairHandle) error
	QueuePairStats() PoolStats
}

// CompletionQueuePool leases completion-queue handles created during daemon startup.
type CompletionQueuePool interface {
	AcquireCompletionQueue(context.Context) (CompletionQueueHandle, error)
	ReleaseCompletionQueue(context.Context, CompletionQueueHandle) error
	CompletionQueueStats() PoolStats
}
