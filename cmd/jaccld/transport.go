package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/tmc/gojaccl/internal/rdma"
	"github.com/tmc/gojaccl/internal/tcpchan"
)

const daemonTransferBytes = 4096 << 7

type daemonTransport struct {
	rank int
	size int

	mr    *rdma.MemoryRegion
	side  *tcpchan.Channel
	conns []*daemonConn
}

type daemonConn struct {
	cq *rdma.CompletionQueue
	qp *rdma.QueuePair
	mu sync.Mutex
}

func openDaemonTransport(ctx context.Context, cfg config, hw *hardware) (*daemonTransport, error) {
	if hw == nil || hw.dev == nil || hw.pd == nil || hw.mr == nil {
		return nil, fmt.Errorf("open daemon transport: nil hardware")
	}
	t := &daemonTransport{
		rank:  cfg.rank,
		size:  cfg.size,
		mr:    hw.mr,
		conns: make([]*daemonConn, cfg.size),
	}
	if err := t.open(ctx, cfg, hw); err != nil {
		_ = t.Close()
		return nil, err
	}
	return t, nil
}

func (t *daemonTransport) open(ctx context.Context, cfg config, hw *hardware) error {
	side, err := tcpchan.New(ctx, cfg.rank, cfg.size, cfg.coordinator)
	if err != nil {
		return fmt.Errorf("side channel: %w", err)
	}
	t.side = side

	local := make([]rdma.Destination, t.size)
	for peer := 0; peer < t.size; peer++ {
		if peer == t.rank {
			continue
		}
		conn, dst, err := openDaemonConn(hw)
		if err != nil {
			return fmt.Errorf("peer %d: %w", peer, err)
		}
		t.conns[peer] = conn
		local[peer] = dst
	}

	payload, err := json.Marshal(local)
	if err != nil {
		return fmt.Errorf("marshal destinations: %w", err)
	}
	allPayloads, err := t.side.AllGather(ctx, payload)
	if err != nil {
		return fmt.Errorf("exchange destinations: %w", err)
	}
	all := make([][]rdma.Destination, t.size)
	for rank, data := range allPayloads {
		if err := json.Unmarshal(data, &all[rank]); err != nil {
			return fmt.Errorf("decode destinations from rank %d: %w", rank, err)
		}
		if len(all[rank]) != t.size {
			return fmt.Errorf("decode destinations from rank %d: got %d entries, want %d", rank, len(all[rank]), t.size)
		}
	}

	for peer, conn := range t.conns {
		if conn == nil {
			continue
		}
		remote := all[peer][t.rank]
		if err := rdma.ReadyToReceive(conn.qp, remote); err != nil {
			return fmt.Errorf("peer %d: %w", peer, err)
		}
		if err := rdma.ReadyToSend(conn.qp, local[peer].PSN); err != nil {
			return fmt.Errorf("peer %d: %w", peer, err)
		}
	}
	if err := t.side.Barrier(ctx); err != nil {
		return fmt.Errorf("ready barrier: %w", err)
	}
	return nil
}

func openDaemonConn(hw *hardware) (*daemonConn, rdma.Destination, error) {
	cq, err := rdma.NewCompletionQueue(hw.dev, 64)
	if err != nil {
		return nil, rdma.Destination{}, err
	}
	conn := &daemonConn{cq: cq}
	defer func() {
		if err != nil {
			_ = conn.close()
		}
	}()
	if conn.qp, err = rdma.NewQueuePair(hw.pd, conn.cq); err != nil {
		return nil, rdma.Destination{}, err
	}
	if err = rdma.InitQueuePair(conn.qp); err != nil {
		return nil, rdma.Destination{}, err
	}
	dst, err := rdma.LocalDestination(conn.qp)
	if err != nil {
		return nil, rdma.Destination{}, err
	}
	return conn, dst, nil
}

func (t *daemonTransport) Barrier(ctx context.Context) error {
	if t == nil || t.side == nil {
		return fmt.Errorf("daemon transport closed")
	}
	return t.side.Barrier(ctx)
}

func (t *daemonTransport) Send(ctx context.Context, peer int, offset, length int64) error {
	start, n, err := t.rangeInMR(offset, length)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	conn, err := t.conn(peer)
	if err != nil {
		return err
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	for off := 0; off < n; off += daemonTransferBytes {
		chunk := min(daemonTransferBytes, n-off)
		if err := rdma.PostSend(conn.qp, t.mr, start+off, chunk, transportWorkID(1, peer)); err != nil {
			return err
		}
		if err := t.poll(ctx, conn, 1); err != nil {
			return err
		}
	}
	return nil
}

func (t *daemonTransport) Recv(ctx context.Context, peer int, offset, length int64) error {
	start, n, err := t.rangeInMR(offset, length)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	conn, err := t.conn(peer)
	if err != nil {
		return err
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	for off := 0; off < n; off += daemonTransferBytes {
		chunk := min(daemonTransferBytes, n-off)
		if err := rdma.PostRecv(conn.qp, t.mr, start+off, chunk, transportWorkID(2, peer)); err != nil {
			return err
		}
		if err := t.poll(ctx, conn, 1); err != nil {
			return err
		}
	}
	return nil
}

func (t *daemonTransport) Close() error {
	if t == nil {
		return nil
	}
	var first error
	for _, conn := range t.conns {
		if conn != nil {
			if err := conn.close(); err != nil && first == nil {
				first = err
			}
		}
	}
	if t.side != nil {
		if err := t.side.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (t *daemonTransport) rangeInMR(offset, length int64) (int, int, error) {
	if t == nil || t.mr == nil {
		return 0, 0, fmt.Errorf("daemon transport closed")
	}
	if offset < 0 || length < 0 {
		return 0, 0, fmt.Errorf("range [%d,%d) is invalid", offset, offset+length)
	}
	end := offset + length
	buf := t.mr.Buffer()
	if end < offset || end > int64(len(buf)) {
		return 0, 0, fmt.Errorf("range [%d,%d) outside registered memory length %d", offset, end, len(buf))
	}
	return int(offset), int(length), nil
}

func (t *daemonTransport) conn(peer int) (*daemonConn, error) {
	if peer < 0 || peer >= t.size {
		return nil, fmt.Errorf("rank %d out of range for size %d", peer, t.size)
	}
	conn := t.conns[peer]
	if conn == nil {
		return nil, fmt.Errorf("rank %d has no RDMA connection", peer)
	}
	return conn, nil
}

func (t *daemonTransport) poll(ctx context.Context, conn *daemonConn, n int) error {
	for i := 0; i < n; i++ {
		if _, err := rdma.PollCompletion(ctx, conn.cq); err != nil {
			return err
		}
	}
	return nil
}

func (c *daemonConn) close() error {
	var first error
	if c.qp != nil {
		if err := c.qp.Close(); err != nil && first == nil {
			first = err
		}
	}
	if c.cq != nil {
		if err := c.cq.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func transportWorkID(kind, peer int) uint64 {
	return uint64(kind<<16 | peer)
}
