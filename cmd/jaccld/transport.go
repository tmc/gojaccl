package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tmc/gojaccl/internal/allocator"
	"github.com/tmc/gojaccl/internal/jaccld/resource"
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

	slab             *allocator.Slab
	mr               *rdma.MemoryRegion
	side             *tcpchan.Channel
	tracker          *keepalive.Tracker
	heartbeatLease   daemonHeartbeatLease
	heartbeatTimeout time.Duration
	heartbeatTTL     time.Duration
	conns            []*daemonConn
}

type daemonConn struct {
	cq              *rdma.CompletionQueue
	qp              *rdma.QueuePair
	remoteHeartbeat daemonHeartbeatLease
	heartbeatWrites atomic.Uint64
	heartbeatErrors atomic.Uint64
	mu              sync.Mutex
}

type daemonHeartbeatLease struct {
	MR        resource.HeartbeatMR
	ExpiresAt time.Time
}

type daemonDestination struct {
	QP           rdma.Destination     `json:"qp"`
	HeartbeatMR  resource.HeartbeatMR `json:"heartbeat_mr,omitempty"`
	HeartbeatTTL time.Duration        `json:"heartbeat_ttl,omitempty"`
}

type daemonExchangeFunc func(context.Context, int64, int64, int64) error

func openDaemonTransport(ctx context.Context, cfg config, side *tcpchan.Channel, slab *allocator.Slab, hw *hardware, tracker *keepalive.Tracker, heartbeat allocator.Lease) (*daemonTransport, error) {
	if hw == nil || hw.dev == nil || hw.pd == nil || hw.mr == nil {
		return nil, fmt.Errorf("open daemon transport: nil hardware")
	}
	if slab == nil {
		return nil, fmt.Errorf("open daemon transport: nil slab")
	}
	if side == nil {
		return nil, fmt.Errorf("open daemon transport: nil side channel")
	}
	ttl := cfg.heartbeatLeaseTTL
	if ttl == 0 {
		ttl = defaultHeartbeatLeaseTTL
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("open daemon transport: heartbeat lease ttl %s must be positive", ttl)
	}
	epoch := uint64(time.Now().UnixNano())
	if epoch == 0 {
		epoch = heartbeat.ID
	}
	hb, err := heartbeatMR(hw.mr.Addr(), hw.mr.RKey(), heartbeat, epoch)
	if err != nil {
		return nil, fmt.Errorf("open daemon transport: heartbeat lease: %w", err)
	}
	t := &daemonTransport{
		rank:             cfg.rank,
		size:             cfg.size,
		slab:             slab,
		mr:               hw.mr,
		tracker:          tracker,
		heartbeatLease:   daemonHeartbeatLease{MR: hb, ExpiresAt: time.Now().Add(ttl)},
		heartbeatTimeout: cfg.heartbeatTimeout,
		heartbeatTTL:     ttl,
		conns:            make([]*daemonConn, cfg.size),
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
			QP:           dst,
			HeartbeatMR:  t.heartbeatLease.MR,
			HeartbeatTTL: t.heartbeatTTL,
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
		if t.tracker != nil {
			lease, err := validateRemoteHeartbeatDestination(remote, time.Now(), conn.remoteHeartbeat.MR.Epoch)
			if err != nil {
				return fmt.Errorf("peer %d heartbeat: %w", peer, err)
			}
			conn.remoteHeartbeat = lease
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

func heartbeatMR(base uint64, rkey uint32, lease allocator.Lease, epoch uint64) (resource.HeartbeatMR, error) {
	if base == 0 {
		return resource.HeartbeatMR{}, fmt.Errorf("missing local memory address")
	}
	if lease.Offset < 0 {
		return resource.HeartbeatMR{}, fmt.Errorf("negative heartbeat offset %d", lease.Offset)
	}
	addr := base + uint64(lease.Offset)
	if addr < base {
		return resource.HeartbeatMR{}, fmt.Errorf("heartbeat address overflow")
	}
	hb := resource.HeartbeatMR{
		Addr:   addr,
		RKey:   rkey,
		Length: lease.Length,
		Epoch:  epoch,
	}
	if err := hb.ValidateForRDMA(); err != nil {
		return resource.HeartbeatMR{}, err
	}
	return hb, nil
}

func validateRemoteHeartbeatDestination(dst daemonDestination, now time.Time, lastEpoch uint64) (daemonHeartbeatLease, error) {
	if err := dst.HeartbeatMR.ValidateForRDMA(); err != nil {
		return daemonHeartbeatLease{}, err
	}
	if dst.HeartbeatTTL <= 0 {
		return daemonHeartbeatLease{}, fmt.Errorf("%w: heartbeat ttl %s must be positive", resource.ErrInvalidRequest, dst.HeartbeatTTL)
	}
	if lastEpoch != 0 && dst.HeartbeatMR.Epoch <= lastEpoch {
		return daemonHeartbeatLease{}, fmt.Errorf("%w: stale heartbeat epoch %d after %d", resource.ErrInvalidRequest, dst.HeartbeatMR.Epoch, lastEpoch)
	}
	return daemonHeartbeatLease{
		MR:        dst.HeartbeatMR,
		ExpiresAt: now.Add(dst.HeartbeatTTL),
	}, nil
}

func (l daemonHeartbeatLease) RDMA(now time.Time) (resource.HeartbeatMR, error) {
	if err := l.MR.ValidateForRDMA(); err != nil {
		return resource.HeartbeatMR{}, err
	}
	if l.ExpiresAt.IsZero() {
		return resource.HeartbeatMR{}, fmt.Errorf("%w: heartbeat lease deadline is zero", resource.ErrInvalidRequest)
	}
	if !l.ExpiresAt.After(now) {
		return resource.HeartbeatMR{}, resource.ErrExpired
	}
	return l.MR, nil
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
		t.touch(peer)
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
		t.touch(peer)
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
			t.touch(peer)
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
	offset, err := t.localHeartbeatOffset()
	if err != nil {
		return err
	}
	start, _, err := t.rangeInMR(offset, 1)
	if err != nil {
		return err
	}
	conn, err := t.conn(peer)
	if err != nil {
		return err
	}
	remote, err := conn.remoteHeartbeat.RDMA(time.Now())
	if err != nil {
		return fmt.Errorf("rank %d heartbeat target: %w", peer, err)
	}
	if !conn.mu.TryLock() {
		return nil
	}
	defer conn.mu.Unlock()
	if err := rdma.PostWrite(conn.qp, t.mr, start, 1, remote.Addr, remote.RKey, transportWorkID(3, peer)); err != nil {
		conn.heartbeatErrors.Add(1)
		return err
	}

	pollCtx := ctx
	var cancel context.CancelFunc
	if t.heartbeatTimeout > 0 {
		pollCtx, cancel = context.WithTimeout(ctx, t.heartbeatTimeout)
		defer cancel()
	}
	if err := t.poll(pollCtx, conn, 1); err != nil {
		conn.heartbeatErrors.Add(1)
		return err
	}
	conn.heartbeatWrites.Add(1)
	return nil
}

func (t *daemonTransport) localHeartbeatOffset() (int64, error) {
	if t == nil || t.mr == nil {
		return 0, fmt.Errorf("heartbeat source: nil memory region")
	}
	local := t.heartbeatLease.MR
	if err := local.ValidateForRDMA(); err != nil {
		return 0, err
	}
	base := t.mr.Addr()
	if base == 0 {
		return 0, fmt.Errorf("heartbeat source: missing memory address")
	}
	if local.Addr < base {
		return 0, fmt.Errorf("heartbeat source address %d before base %d", local.Addr, base)
	}
	off := local.Addr - base
	if off > uint64(maxInt64) {
		return 0, fmt.Errorf("heartbeat source offset overflows")
	}
	return int64(off), nil
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

func (t *daemonTransport) touch(peer int) {
	if t != nil && t.tracker != nil {
		t.tracker.Touch(t.routeID(peer))
	}
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
