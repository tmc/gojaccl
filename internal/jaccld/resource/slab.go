package resource

import (
	"context"
	"fmt"
	"strconv"

	"github.com/tmc/gojaccl/internal/allocator"
)

// SlabMRPool leases windows from an allocator.Slab.
type SlabMRPool struct {
	slab *allocator.Slab
}

// NewSlabMRPool returns an MRPool backed by slab.
func NewSlabMRPool(slab *allocator.Slab) (*SlabMRPool, error) {
	if slab == nil {
		return nil, fmt.Errorf("new slab mr pool: nil slab")
	}
	return &SlabMRPool{slab: slab}, nil
}

// AllocMR leases one logical memory-region window from the slab.
func (p *SlabMRPool) AllocMR(ctx context.Context, n int64) (MRWindow, error) {
	if err := ctx.Err(); err != nil {
		return MRWindow{}, err
	}
	lease, err := p.slab.Alloc(n)
	if err != nil {
		return MRWindow{}, err
	}
	return MRWindow{
		SlabID: strconv.FormatUint(lease.ID, 10),
		Offset: lease.Offset,
		Length: lease.Length,
	}, nil
}

// FreeMR releases a previously leased memory-region window.
func (p *SlabMRPool) FreeMR(ctx context.Context, w MRWindow) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id, err := strconv.ParseUint(w.SlabID, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: memory window slab id %q", ErrInvalidRequest, w.SlabID)
	}
	return p.slab.Free(id)
}

// MRStats reports current slab use as resource-pool stats.
func (p *SlabMRPool) MRStats() PoolStats {
	st := p.slab.Stats()
	return PoolStats{
		InUse:          st.Leases,
		Available:      st.FreeRanges,
		BytesInUse:     st.Used,
		BytesAvailable: st.Free,
	}
}
