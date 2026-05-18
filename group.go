package jaccl

import (
	"context"
	"sync"
)

// Group is a live communicator.
type Group struct {
	rank int
	size int

	b backend

	op     chan struct{}
	closed chan struct{}
	once   sync.Once
}

// NewGroup initializes a communicator from cfg.
func NewGroup(ctx context.Context, cfg Config) (*Group, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := cfg.validate(); err != nil {
		return nil, wrapError(cfg.Rank, "new group", err)
	}
	size, err := cfg.groupSize()
	if err != nil {
		return nil, wrapError(cfg.Rank, "new group", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, wrapError(cfg.Rank, "new group", err)
	}
	b, err := newBackend(ctx, cfg)
	if err != nil {
		return nil, wrapError(cfg.Rank, "new group", err)
	}
	return &Group{
		rank:   cfg.Rank,
		size:   size,
		b:      b,
		op:     make(chan struct{}, 1),
		closed: make(chan struct{}),
	}, nil
}

// NewGroupFromEnv reads configuration from the environment and initializes a group.
func NewGroupFromEnv(ctx context.Context) (*Group, error) {
	cfg, err := ConfigFromEnv()
	if err != nil {
		return nil, wrapError(-1, "new group from env", err)
	}
	g, err := NewGroup(ctx, cfg)
	if err != nil {
		return nil, wrapError(cfg.Rank, "new group from env", err)
	}
	return g, nil
}

// Rank reports the local rank.
func (g *Group) Rank() int {
	if g == nil {
		return -1
	}
	return g.rank
}

// Size reports the number of ranks in the group.
func (g *Group) Size() int {
	if g == nil {
		return 0
	}
	return g.size
}

// Close releases group resources. It is safe to call Close more than once.
func (g *Group) Close() error {
	if g == nil {
		return nil
	}
	var err error
	g.once.Do(func() {
		close(g.closed)
		if g.b != nil {
			err = g.b.close()
		}
	})
	return wrapError(g.rank, "close", err)
}

func (g *Group) do(ctx context.Context, op string, fn func(backend) error) error {
	lease, err := g.begin(ctx, op)
	if err != nil {
		return err
	}
	defer lease.release()
	return lease.do(fn)
}

type groupOperation struct {
	g        *Group
	name     string
	released bool
}

func (g *Group) begin(ctx context.Context, op string) (*groupOperation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if g == nil {
		return nil, wrapError(-1, op, ErrClosed)
	}
	if err := g.acquire(ctx); err != nil {
		return nil, wrapError(g.rank, op, err)
	}
	if g.b == nil {
		g.release()
		return nil, wrapError(g.rank, op, ErrClosed)
	}
	return &groupOperation{g: g, name: op}, nil
}

func (op *groupOperation) do(fn func(backend) error) error {
	if op == nil || op.released || op.g == nil || op.g.b == nil {
		return wrapError(-1, "operation", ErrClosed)
	}
	return wrapError(op.g.rank, op.name, fn(op.g.b))
}

func (op *groupOperation) release() {
	if op == nil || op.released || op.g == nil {
		return
	}
	op.released = true
	op.g.release()
}

func (g *Group) acquire(ctx context.Context) error {
	select {
	case <-g.closed:
		return ErrClosed
	default:
	}
	select {
	case g.op <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-g.closed:
		return ErrClosed
	}
	select {
	case <-g.closed:
		g.release()
		return ErrClosed
	default:
		return nil
	}
}

func (g *Group) release() {
	select {
	case <-g.op:
	default:
	}
}
