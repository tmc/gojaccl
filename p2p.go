package jaccl

import (
	"context"
	"fmt"
)

// Barrier waits until every rank reaches the same barrier.
func (g *Group) Barrier(ctx context.Context) error {
	return g.do(ctx, "barrier", func(b backend) error {
		return b.barrier(ctx)
	})
}

// Send sends opaque bytes to dst.
func (g *Group) Send(ctx context.Context, dst int, src []byte) error {
	if err := g.checkRank("send", dst); err != nil {
		return err
	}
	return g.do(ctx, "send", func(b backend) error {
		return b.send(ctx, dst, src)
	})
}

// Recv receives opaque bytes from src into dst.
func (g *Group) Recv(ctx context.Context, src int, dst []byte) error {
	if err := g.checkRank("recv", src); err != nil {
		return err
	}
	return g.do(ctx, "recv", func(b backend) error {
		return b.recv(ctx, src, dst)
	})
}

func (g *Group) checkRank(op string, rank int) error {
	if g == nil {
		return wrapError(-1, op, ErrClosed)
	}
	if rank < 0 || rank >= g.size {
		return wrapError(g.rank, op, fmt.Errorf("rank %d out of range for size %d", rank, g.size))
	}
	if rank == g.rank {
		return wrapError(g.rank, op, fmt.Errorf("peer rank %d is self", rank))
	}
	return nil
}
