package jaccl

import (
	"context"
	"fmt"
	"time"

	"github.com/tmc/gojaccl/internal/allocator"
	"github.com/tmc/gojaccl/internal/ipc"
	"github.com/tmc/gojaccl/internal/reduce"
)

type daemonBackend struct {
	client  *ipc.Client
	mapping *ipc.Mapping
}

func newDaemonBackend(ctx context.Context, cfg Config) (backend, error) {
	client, err := ipc.Dial(ctx, cfg.daemonSocket())
	if err != nil {
		return nil, err
	}
	mapping, err := client.Map(ctx)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return &daemonBackend{client: client, mapping: mapping}, nil
}

func (b *daemonBackend) barrier(ctx context.Context) error {
	return b.client.Barrier(ctx)
}

func (b *daemonBackend) send(ctx context.Context, dst int, src []byte) (err error) {
	if len(src) == 0 {
		return nil
	}
	lease, err := b.client.Alloc(ctx, int64(len(src)))
	if err != nil {
		return err
	}
	defer b.free(&err, lease.ID)
	buf, err := b.bytes(lease)
	if err != nil {
		return err
	}
	copy(buf, src)
	return b.client.Send(ctx, dst, lease)
}

func (b *daemonBackend) recv(ctx context.Context, src int, dst []byte) (err error) {
	if len(dst) == 0 {
		return nil
	}
	lease, err := b.client.Alloc(ctx, int64(len(dst)))
	if err != nil {
		return err
	}
	defer b.free(&err, lease.ID)
	if err := b.client.Recv(ctx, src, lease); err != nil {
		return err
	}
	buf, err := b.bytes(lease)
	if err != nil {
		return err
	}
	copy(dst, buf)
	return nil
}

func (b *daemonBackend) allReduce(ctx context.Context, op reductionOp, dt reduce.DType, dst, src []byte) (err error) {
	if len(src) == 0 {
		return nil
	}
	srcLease, err := b.client.Alloc(ctx, int64(len(src)))
	if err != nil {
		return err
	}
	defer b.free(&err, srcLease.ID)
	dstLease, err := b.client.Alloc(ctx, int64(len(dst)))
	if err != nil {
		return err
	}
	defer b.free(&err, dstLease.ID)
	srcBuf, err := b.bytes(srcLease)
	if err != nil {
		return err
	}
	copy(srcBuf, src)
	work, err := b.client.SubmitReduce(ctx, int(op), int(dt), dstLease, srcLease)
	if err != nil {
		return err
	}
	if err := b.client.WaitWork(ctx, work); err != nil {
		return err
	}
	dstBuf, err := b.bytes(dstLease)
	if err != nil {
		return err
	}
	copy(dst, dstBuf)
	return nil
}

func (b *daemonBackend) allGather(ctx context.Context, elemSize int, dst, src []byte) (err error) {
	if len(src) == 0 {
		return nil
	}
	srcLease, err := b.client.Alloc(ctx, int64(len(src)))
	if err != nil {
		return err
	}
	defer b.free(&err, srcLease.ID)
	dstLease, err := b.client.Alloc(ctx, int64(len(dst)))
	if err != nil {
		return err
	}
	defer b.free(&err, dstLease.ID)
	srcBuf, err := b.bytes(srcLease)
	if err != nil {
		return err
	}
	copy(srcBuf, src)
	work, err := b.client.SubmitGather(ctx, elemSize, dstLease, srcLease)
	if err != nil {
		return err
	}
	if err := b.client.WaitWork(ctx, work); err != nil {
		return err
	}
	dstBuf, err := b.bytes(dstLease)
	if err != nil {
		return err
	}
	copy(dst, dstBuf)
	return nil
}

func (b *daemonBackend) close() error {
	var first error
	if b.mapping != nil {
		if err := b.mapping.Close(); err != nil && first == nil {
			first = err
		}
	}
	if b.client != nil {
		if err := b.client.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (b *daemonBackend) bytes(lease allocator.Lease) ([]byte, error) {
	if b.mapping == nil {
		return nil, fmt.Errorf("daemon backend mapping is closed")
	}
	if lease.Offset < 0 || lease.Length < 0 {
		return nil, fmt.Errorf("lease %d has invalid range [%d,%d)", lease.ID, lease.Offset, lease.Offset+lease.Length)
	}
	end := lease.Offset + lease.Length
	if end < lease.Offset || end > int64(len(b.mapping.Data)) {
		return nil, fmt.Errorf("lease %d range [%d,%d) outside mapping length %d", lease.ID, lease.Offset, end, len(b.mapping.Data))
	}
	return b.mapping.Data[int(lease.Offset):int(end)], nil
}

func (b *daemonBackend) free(errp *error, id uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.client.Free(ctx, id); err != nil && *errp == nil {
		*errp = err
	}
}
