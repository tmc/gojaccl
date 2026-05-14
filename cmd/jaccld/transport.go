package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/tmc/gojaccl/internal/allocator"
	"github.com/tmc/gojaccl/internal/keepalive"
	"github.com/tmc/gojaccl/internal/rdma"
	"github.com/tmc/gojaccl/internal/reduce"
	"github.com/tmc/gojaccl/internal/tcpchan"
)

const daemonTransferBytes = 4096 << 7

const maxInt64 = int64(^uint64(0) >> 1)

const (
	daemonReductionSum = iota
	daemonReductionMax
	daemonReductionMin
)

type daemonTransport struct {
	rank int
	size int

	slab            *allocator.Slab
	mr              *rdma.MemoryRegion
	side            *tcpchan.Channel
	tracker         *keepalive.Tracker
	heartbeatOffset int64
	conns           []*daemonConn
}

type daemonConn struct {
	cq                  *rdma.CompletionQueue
	qp                  *rdma.QueuePair
	remoteHeartbeatAddr uint64
	remoteRKey          uint32
	mu                  sync.Mutex
}

type daemonDestination struct {
	QP              rdma.Destination `json:"qp"`
	MRAddr          uint64           `json:"mr_addr"`
	RKey            uint32           `json:"rkey"`
	HeartbeatOffset int64            `json:"heartbeat_offset"`
}

type daemonExchangeFunc func(context.Context, int64, int64, int64) error

func openDaemonTransport(ctx context.Context, cfg config, side *tcpchan.Channel, slab *allocator.Slab, hw *hardware, tracker *keepalive.Tracker, heartbeatOffset int64) (*daemonTransport, error) {
	if hw == nil || hw.dev == nil || hw.pd == nil || hw.mr == nil {
		return nil, fmt.Errorf("open daemon transport: nil hardware")
	}
	if slab == nil {
		return nil, fmt.Errorf("open daemon transport: nil slab")
	}
	if side == nil {
		return nil, fmt.Errorf("open daemon transport: nil side channel")
	}
	t := &daemonTransport{
		rank:            cfg.rank,
		size:            cfg.size,
		slab:            slab,
		mr:              hw.mr,
		tracker:         tracker,
		heartbeatOffset: heartbeatOffset,
		conns:           make([]*daemonConn, cfg.size),
	}
	if err := t.open(ctx, cfg, hw, side); err != nil {
		_ = t.Close()
		return nil, err
	}
	return t, nil
}

func (t *daemonTransport) open(ctx context.Context, cfg config, hw *hardware, side *tcpchan.Channel) error {
	t.side = side

	local := make([]daemonDestination, t.size)
	for peer := 0; peer < t.size; peer++ {
		if peer == t.rank {
			continue
		}
		conn, dst, err := openDaemonConn(hw)
		if err != nil {
			return fmt.Errorf("peer %d: %w", peer, err)
		}
		t.conns[peer] = conn
		local[peer] = daemonDestination{
			QP:              dst,
			MRAddr:          hw.mr.Addr(),
			RKey:            hw.mr.RKey(),
			HeartbeatOffset: t.heartbeatOffset,
		}
	}

	payload, err := json.Marshal(local)
	if err != nil {
		return fmt.Errorf("marshal destinations: %w", err)
	}
	allPayloads, err := t.side.AllGather(ctx, payload)
	if err != nil {
		return fmt.Errorf("exchange destinations: %w", err)
	}
	all := make([][]daemonDestination, t.size)
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
		if err := rdma.ReadyToReceive(conn.qp, remote.QP); err != nil {
			return fmt.Errorf("peer %d: %w", peer, err)
		}
		if err := rdma.ReadyToSend(conn.qp, local[peer].QP.PSN); err != nil {
			return fmt.Errorf("peer %d: %w", peer, err)
		}
		addr, err := heartbeatAddress(remote)
		if err != nil {
			return fmt.Errorf("peer %d heartbeat: %w", peer, err)
		}
		conn.remoteHeartbeatAddr = addr
		conn.remoteRKey = remote.RKey
		if t.tracker != nil {
			peer := peer
			if err := t.tracker.Add(t.routeID(peer), keepalive.SenderFunc(func(ctx context.Context) error {
				return t.heartbeat(ctx, peer)
			})); err != nil {
				return err
			}
		}
	}
	if err := t.side.Barrier(ctx); err != nil {
		return fmt.Errorf("ready barrier: %w", err)
	}
	return nil
}

func heartbeatAddress(dst daemonDestination) (uint64, error) {
	if dst.MRAddr == 0 {
		return 0, fmt.Errorf("missing remote memory address")
	}
	if dst.HeartbeatOffset < 0 {
		return 0, fmt.Errorf("negative remote heartbeat offset %d", dst.HeartbeatOffset)
	}
	addr := dst.MRAddr + uint64(dst.HeartbeatOffset)
	if addr < dst.MRAddr {
		return 0, fmt.Errorf("remote heartbeat address overflow")
	}
	return addr, nil
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

func (t *daemonTransport) AllReduce(ctx context.Context, op, dt int, dstOffset, dstLength, srcOffset, srcLength int64) error {
	return t.runMeshReduce(ctx, op, reduce.DType(dt), dstOffset, dstLength, srcOffset, srcLength, t.exchange)
}

func (t *daemonTransport) AllGather(ctx context.Context, elemSize int, dstOffset, dstLength, srcOffset, srcLength int64) error {
	return t.runMeshGather(ctx, elemSize, dstOffset, dstLength, srcOffset, srcLength, t.exchange)
}

func (t *daemonTransport) runMeshReduce(ctx context.Context, op int, dt reduce.DType, dstOffset, dstLength, srcOffset, srcLength int64, exchange daemonExchangeFunc) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if srcLength == 0 {
		return nil
	}
	if t == nil || t.slab == nil {
		return fmt.Errorf("daemon reduce: nil slab")
	}
	dstStart, _, err := t.rangeInMR(dstOffset, dstLength)
	if err != nil {
		return err
	}
	srcStart, srcN, err := t.rangeInMR(srcOffset, srcLength)
	if err != nil {
		return err
	}
	if dstLength < srcLength {
		return fmt.Errorf("daemon reduce: destination length %d, want at least %d", dstLength, srcLength)
	}
	if t.size <= 0 {
		return fmt.Errorf("daemon reduce: size %d must be positive", t.size)
	}
	if srcLength > maxInt64/int64(t.size) {
		return fmt.Errorf("daemon reduce: scratch size overflows")
	}
	scratch, err := t.slab.Alloc(srcLength * int64(t.size))
	if err != nil {
		return fmt.Errorf("daemon reduce: alloc scratch: %w", err)
	}
	defer t.slab.Free(scratch.ID)

	buf := t.bytes()
	copy(buf[dstStart:dstStart+srcN], buf[srcStart:srcStart+srcN])
	if t.size > 1 {
		if exchange == nil {
			return fmt.Errorf("daemon reduce: nil exchange")
		}
		if err := exchange(ctx, scratch.Offset, srcOffset, srcLength); err != nil {
			return err
		}
		for peer := 0; peer < t.size; peer++ {
			if peer == t.rank {
				continue
			}
			peerStart, peerN, err := t.rangeInMR(peerOffset(scratch.Offset, peer, srcLength), srcLength)
			if err != nil {
				return err
			}
			if err := applyDaemonReduction(op, dt, buf[dstStart:dstStart+srcN], buf[peerStart:peerStart+peerN]); err != nil {
				return fmt.Errorf("reduce rank %d: %w", peer, err)
			}
		}
	}
	return nil
}

func (t *daemonTransport) runMeshGather(ctx context.Context, elemSize int, dstOffset, dstLength, srcOffset, srcLength int64, exchange daemonExchangeFunc) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if srcLength == 0 {
		return nil
	}
	if elemSize <= 0 {
		return fmt.Errorf("daemon gather: element size %d must be positive", elemSize)
	}
	if srcLength%int64(elemSize) != 0 {
		return fmt.Errorf("daemon gather: source length %d is not a multiple of element size %d", srcLength, elemSize)
	}
	if t.size <= 0 {
		return fmt.Errorf("daemon gather: size %d must be positive", t.size)
	}
	if srcLength > maxInt64/int64(t.size) {
		return fmt.Errorf("daemon gather: destination size overflows")
	}
	want := srcLength * int64(t.size)
	if dstLength < want {
		return fmt.Errorf("daemon gather: destination length %d, want at least %d", dstLength, want)
	}
	dstStart, _, err := t.rangeInMR(dstOffset, dstLength)
	if err != nil {
		return err
	}
	srcStart, srcN, err := t.rangeInMR(srcOffset, srcLength)
	if err != nil {
		return err
	}
	localStart := dstStart + t.rank*srcN
	buf := t.bytes()
	copy(buf[localStart:localStart+srcN], buf[srcStart:srcStart+srcN])
	if t.size == 1 {
		return nil
	}
	if exchange == nil {
		return fmt.Errorf("daemon gather: nil exchange")
	}
	return exchange(ctx, dstOffset, srcOffset, srcLength)
}

func (t *daemonTransport) exchange(ctx context.Context, dstBase, srcOffset, length int64) error {
	srcStart, n, err := t.rangeInMR(srcOffset, length)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	locked, err := t.lockConns()
	if err != nil {
		return err
	}
	defer unlockDaemonConns(locked)

	for off := 0; off < n; off += daemonTransferBytes {
		chunk := min(daemonTransferBytes, n-off)
		for peer, conn := range t.conns {
			if peer == t.rank || conn == nil {
				continue
			}
			dstStart, _, err := t.rangeInMR(peerOffset(dstBase, peer, int64(n))+int64(off), int64(chunk))
			if err != nil {
				return err
			}
			if err := rdma.PostRecv(conn.qp, t.mr, dstStart, chunk, transportWorkID(2, peer)); err != nil {
				return fmt.Errorf("peer %d post recv: %w", peer, err)
			}
		}
		for peer, conn := range t.conns {
			if peer == t.rank || conn == nil {
				continue
			}
			if err := rdma.PostSend(conn.qp, t.mr, srcStart+off, chunk, transportWorkID(1, peer)); err != nil {
				return fmt.Errorf("peer %d post send: %w", peer, err)
			}
		}
		for peer, conn := range t.conns {
			if peer == t.rank || conn == nil {
				continue
			}
			if err := t.poll(ctx, conn, 2); err != nil {
				return fmt.Errorf("peer %d poll: %w", peer, err)
			}
		}
	}
	return nil
}

func (t *daemonTransport) Close() error {
	if t == nil {
		return nil
	}
	var first error
	for peer, conn := range t.conns {
		if t.tracker != nil {
			t.tracker.Remove(t.routeID(peer))
		}
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
	buf := t.bytes()
	if buf == nil {
		return 0, 0, fmt.Errorf("daemon transport closed")
	}
	if offset < 0 || length < 0 {
		return 0, 0, fmt.Errorf("range [%d,%d) is invalid", offset, offset+length)
	}
	end := offset + length
	if end < offset || end > int64(len(buf)) {
		return 0, 0, fmt.Errorf("range [%d,%d) outside registered memory length %d", offset, end, len(buf))
	}
	return int(offset), int(length), nil
}

func (t *daemonTransport) bytes() []byte {
	if t == nil {
		return nil
	}
	if t.slab != nil {
		return t.slab.Bytes()
	}
	if t.mr != nil {
		return t.mr.Buffer()
	}
	return nil
}

func (t *daemonTransport) heartbeat(ctx context.Context, peer int) error {
	start, _, err := t.rangeInMR(t.heartbeatOffset, 1)
	if err != nil {
		return err
	}
	conn, err := t.conn(peer)
	if err != nil {
		return err
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.remoteHeartbeatAddr == 0 {
		return fmt.Errorf("rank %d has no heartbeat target", peer)
	}
	if err := rdma.PostWrite(conn.qp, t.mr, start, 1, conn.remoteHeartbeatAddr, conn.remoteRKey, transportWorkID(3, peer)); err != nil {
		return err
	}
	return t.poll(ctx, conn, 1)
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

func (t *daemonTransport) lockConns() ([]*daemonConn, error) {
	locked := make([]*daemonConn, 0, t.size-1)
	for peer := 0; peer < t.size; peer++ {
		if peer == t.rank {
			continue
		}
		conn, err := t.conn(peer)
		if err != nil {
			unlockDaemonConns(locked)
			return nil, err
		}
		if hasDaemonConn(locked, conn) {
			continue
		}
		conn.mu.Lock()
		locked = append(locked, conn)
	}
	return locked, nil
}

func unlockDaemonConns(conns []*daemonConn) {
	for i := len(conns) - 1; i >= 0; i-- {
		conns[i].mu.Unlock()
	}
}

func (t *daemonTransport) routeID(peer int) string {
	return fmt.Sprintf("rank-%d", peer)
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
	c.mu.Lock()
	defer c.mu.Unlock()
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

func applyDaemonReduction(op int, dt reduce.DType, dst, src []byte) error {
	switch op {
	case daemonReductionSum:
		return reduce.Sum(dt, dst, src)
	case daemonReductionMax:
		return reduce.Max(dt, dst, src)
	case daemonReductionMin:
		return reduce.Min(dt, dst, src)
	default:
		return fmt.Errorf("unknown reduction op %d", op)
	}
}

func peerOffset(base int64, peer int, length int64) int64 {
	return base + int64(peer)*length
}

func hasDaemonConn(conns []*daemonConn, conn *daemonConn) bool {
	for _, c := range conns {
		if c == conn {
			return true
		}
	}
	return false
}
