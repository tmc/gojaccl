package jaccl

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/tmc/gojaccl/internal/rdma"
	"github.com/tmc/gojaccl/internal/reduce"
	"github.com/tmc/gojaccl/internal/tcpchan"
	"github.com/tmc/gojaccl/internal/topology"
)

type reductionOp int

const (
	reductionSum reductionOp = iota
	reductionMax
	reductionMin
)

const rdmaStagingBytes = 4096 << 7

type backend interface {
	barrier(context.Context) error
	send(context.Context, int, []byte) error
	recv(context.Context, int, []byte) error
	allReduce(context.Context, reductionOp, reduce.DType, []byte, []byte) error
	allGather(context.Context, int, []byte, []byte) error
	close() error
}

var backendFactory = newRDMABackend

// Available reports whether the platform RDMA backend is available.
func Available() (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return rdma.Available()
}

func newBackend(ctx context.Context, cfg Config) (backend, error) {
	if cfg.backendMode() == BackendDaemon {
		return nil, ErrDaemonBackend
	}
	return backendFactory(ctx, cfg)
}

type rdmaBackend struct {
	rank int
	size int
	topo topology.Topology

	side  *tcpchan.Channel
	conns []*rdmaConn
}

type rdmaConn struct {
	dev    *rdma.Device
	pd     *rdma.ProtectionDomain
	cq     *rdma.CompletionQueue
	qp     *rdma.QueuePair
	sendMR *rdma.MemoryRegion
	recvMR *rdma.MemoryRegion
	mu     sync.Mutex
}

func newRDMABackend(ctx context.Context, cfg Config) (backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !rdma.Available() {
		return nil, rdma.ErrUnavailable
	}
	topo, err := topology.Choose(cfg.Devices, cfg.PreferRing)
	if err != nil {
		return nil, err
	}

	b := &rdmaBackend{
		rank:  cfg.Rank,
		size:  len(cfg.Devices),
		topo:  topo,
		conns: make([]*rdmaConn, len(cfg.Devices)),
	}
	if err := b.open(ctx, cfg); err != nil {
		_ = b.close()
		return nil, err
	}
	return b, nil
}

func (b *rdmaBackend) open(ctx context.Context, cfg Config) error {
	side, err := tcpchan.New(ctx, cfg.Rank, len(cfg.Devices), cfg.Coordinator)
	if err != nil {
		return fmt.Errorf("side channel: %w", err)
	}
	b.side = side

	local := make([]rdma.Destination, b.size)
	for peer := 0; peer < b.size; peer++ {
		if peer == b.rank {
			continue
		}
		device := b.deviceForPeer(cfg, peer)
		if device == "" {
			continue
		}
		conn, dst, err := openRDMAConn(device)
		if err != nil {
			return fmt.Errorf("peer %d device %q: %w", peer, device, err)
		}
		b.conns[peer] = conn
		local[peer] = dst
	}

	payload, err := json.Marshal(local)
	if err != nil {
		return fmt.Errorf("marshal destinations: %w", err)
	}
	allPayloads, err := b.side.AllGather(ctx, payload)
	if err != nil {
		return fmt.Errorf("exchange destinations: %w", err)
	}
	all := make([][]rdma.Destination, b.size)
	for rank, data := range allPayloads {
		if err := json.Unmarshal(data, &all[rank]); err != nil {
			return fmt.Errorf("decode destinations from rank %d: %w", rank, err)
		}
		if len(all[rank]) != b.size {
			return fmt.Errorf("decode destinations from rank %d: got %d entries, want %d", rank, len(all[rank]), b.size)
		}
	}

	for peer, conn := range b.conns {
		if conn == nil {
			continue
		}
		remote := all[peer][b.rank]
		if err := rdma.ReadyToReceive(conn.qp, remote); err != nil {
			return fmt.Errorf("peer %d: %w", peer, err)
		}
		if err := rdma.ReadyToSend(conn.qp, local[peer].PSN); err != nil {
			return fmt.Errorf("peer %d: %w", peer, err)
		}
	}
	if err := b.side.Barrier(ctx); err != nil {
		return fmt.Errorf("ready barrier: %w", err)
	}
	return nil
}

func openRDMAConn(device string) (*rdmaConn, rdma.Destination, error) {
	dev, err := rdma.OpenDevice(device)
	if err != nil {
		return nil, rdma.Destination{}, err
	}
	conn := &rdmaConn{dev: dev}
	defer func() {
		if err != nil {
			_ = conn.close()
		}
	}()
	if conn.pd, err = rdma.NewProtectionDomain(dev); err != nil {
		return nil, rdma.Destination{}, err
	}
	if conn.cq, err = rdma.NewCompletionQueue(dev, 64); err != nil {
		return nil, rdma.Destination{}, err
	}
	if conn.qp, err = rdma.NewQueuePair(conn.pd, conn.cq); err != nil {
		return nil, rdma.Destination{}, err
	}
	if conn.sendMR, err = rdma.NewMemoryRegion(conn.pd, rdmaStagingBytes); err != nil {
		return nil, rdma.Destination{}, err
	}
	if conn.recvMR, err = rdma.NewMemoryRegion(conn.pd, rdmaStagingBytes); err != nil {
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

func (b *rdmaBackend) deviceForPeer(cfg Config, peer int) string {
	if peer < 0 || peer >= len(cfg.Devices[b.rank]) || len(cfg.Devices[b.rank][peer]) == 0 {
		return ""
	}
	return cfg.Devices[b.rank][peer][0]
}

func (b *rdmaBackend) barrier(ctx context.Context) error {
	return b.side.Barrier(ctx)
}

func (b *rdmaBackend) send(ctx context.Context, dst int, src []byte) error {
	if len(src) == 0 {
		return nil
	}
	conn, err := b.conn(dst)
	if err != nil {
		return err
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	for off := 0; off < len(src); off += rdmaStagingBytes {
		n := min(rdmaStagingBytes, len(src)-off)
		copy(conn.sendMR.Buffer()[:n], src[off:off+n])
		if err := rdma.PostSend(conn.qp, conn.sendMR, 0, n, workID(1, dst)); err != nil {
			return err
		}
		if err := b.poll(ctx, conn, 1); err != nil {
			return err
		}
	}
	return nil
}

func (b *rdmaBackend) recv(ctx context.Context, src int, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	conn, err := b.conn(src)
	if err != nil {
		return err
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	for off := 0; off < len(dst); off += rdmaStagingBytes {
		n := min(rdmaStagingBytes, len(dst)-off)
		if err := rdma.PostRecv(conn.qp, conn.recvMR, 0, n, workID(2, src)); err != nil {
			return err
		}
		if err := b.poll(ctx, conn, 1); err != nil {
			return err
		}
		copy(dst[off:off+n], conn.recvMR.Buffer()[:n])
	}
	return nil
}

func (b *rdmaBackend) allReduce(ctx context.Context, op reductionOp, dt reduce.DType, dst, src []byte) error {
	if len(src) == 0 {
		return nil
	}
	if b.topo == topology.Ring && b.size > 2 {
		return b.ringAllReduce(ctx, op, dt, dst, src)
	}
	recvs, err := b.exchange(ctx, src)
	if err != nil {
		return err
	}
	copy(dst, src)
	for peer := 0; peer < b.size; peer++ {
		if peer == b.rank {
			continue
		}
		if err := applyReduction(op, dt, dst, recvs[peer]); err != nil {
			return fmt.Errorf("reduce rank %d: %w", peer, err)
		}
	}
	return nil
}

func (b *rdmaBackend) allGather(ctx context.Context, elemSize int, dst, src []byte) error {
	if len(src) == 0 {
		return nil
	}
	if b.topo == topology.Ring && b.size > 2 {
		return b.ringAllGather(ctx, dst, src)
	}
	recvs, err := b.exchange(ctx, src)
	if err != nil {
		return err
	}
	copy(dst[b.rank*len(src):(b.rank+1)*len(src)], src)
	for peer := 0; peer < b.size; peer++ {
		if peer == b.rank {
			continue
		}
		copy(dst[peer*len(src):(peer+1)*len(src)], recvs[peer])
	}
	return nil
}

func (b *rdmaBackend) ringAllReduce(ctx context.Context, op reductionOp, dt reduce.DType, dst, src []byte) error {
	values, err := b.ringGather(ctx, src)
	if err != nil {
		return err
	}
	copy(dst, src)
	for peer := 0; peer < b.size; peer++ {
		if peer == b.rank {
			continue
		}
		if err := applyReduction(op, dt, dst, values[peer]); err != nil {
			return fmt.Errorf("reduce rank %d: %w", peer, err)
		}
	}
	return nil
}

func (b *rdmaBackend) ringAllGather(ctx context.Context, dst, src []byte) error {
	values, err := b.ringGather(ctx, src)
	if err != nil {
		return err
	}
	for peer, value := range values {
		copy(dst[peer*len(src):(peer+1)*len(src)], value)
	}
	return nil
}

func (b *rdmaBackend) ringGather(ctx context.Context, src []byte) ([][]byte, error) {
	left := (b.rank - 1 + b.size) % b.size
	right := (b.rank + 1) % b.size
	recvConn, err := b.conn(left)
	if err != nil {
		return nil, err
	}
	sendConn, err := b.conn(right)
	if err != nil {
		return nil, err
	}
	locked := lockConns(recvConn, sendConn)
	defer unlockConns(locked)

	values := make([][]byte, b.size)
	values[b.rank] = append([]byte(nil), src...)
	send := append([]byte(nil), src...)
	for step := 0; step < b.size-1; step++ {
		recvRank := mod(b.rank-step-1, b.size)
		recv := make([]byte, len(src))
		for off := 0; off < len(src); off += rdmaStagingBytes {
			n := min(rdmaStagingBytes, len(src)-off)
			if err := rdma.PostRecv(recvConn.qp, recvConn.recvMR, 0, n, workID(2, left)); err != nil {
				return nil, fmt.Errorf("rank %d post recv: %w", left, err)
			}
			copy(sendConn.sendMR.Buffer()[:n], send[off:off+n])
			if err := rdma.PostSend(sendConn.qp, sendConn.sendMR, 0, n, workID(1, right)); err != nil {
				return nil, fmt.Errorf("rank %d post send: %w", right, err)
			}
			if err := b.poll(ctx, sendConn, 1); err != nil {
				return nil, fmt.Errorf("rank %d send poll: %w", right, err)
			}
			if err := b.poll(ctx, recvConn, 1); err != nil {
				return nil, fmt.Errorf("rank %d recv poll: %w", left, err)
			}
			copy(recv[off:off+n], recvConn.recvMR.Buffer()[:n])
		}
		values[recvRank] = recv
		send = recv
	}
	return values, nil
}

func (b *rdmaBackend) exchange(ctx context.Context, src []byte) ([][]byte, error) {
	recvs := make([][]byte, b.size)
	locked := make([]*rdmaConn, 0, b.size-1)
	defer unlockConns(locked)
	for peer, conn := range b.conns {
		if peer == b.rank || conn == nil {
			continue
		}
		conn.mu.Lock()
		locked = append(locked, conn)
		recvs[peer] = make([]byte, len(src))
	}

	for off := 0; off < len(src); off += rdmaStagingBytes {
		n := min(rdmaStagingBytes, len(src)-off)
		for peer, conn := range b.conns {
			if peer == b.rank || conn == nil {
				continue
			}
			if err := rdma.PostRecv(conn.qp, conn.recvMR, 0, n, workID(2, peer)); err != nil {
				return nil, fmt.Errorf("peer %d post recv: %w", peer, err)
			}
		}
		for peer, conn := range b.conns {
			if peer == b.rank || conn == nil {
				continue
			}
			copy(conn.sendMR.Buffer()[:n], src[off:off+n])
			if err := rdma.PostSend(conn.qp, conn.sendMR, 0, n, workID(1, peer)); err != nil {
				return nil, fmt.Errorf("peer %d post send: %w", peer, err)
			}
		}
		for peer, conn := range b.conns {
			if peer == b.rank || conn == nil {
				continue
			}
			if err := b.poll(ctx, conn, 2); err != nil {
				return nil, fmt.Errorf("peer %d poll: %w", peer, err)
			}
			copy(recvs[peer][off:off+n], conn.recvMR.Buffer()[:n])
		}
	}
	return recvs, nil
}

func lockConns(conns ...*rdmaConn) []*rdmaConn {
	locked := make([]*rdmaConn, 0, len(conns))
	for _, conn := range conns {
		if conn == nil || hasConn(locked, conn) {
			continue
		}
		conn.mu.Lock()
		locked = append(locked, conn)
	}
	return locked
}

func unlockConns(conns []*rdmaConn) {
	for i := len(conns) - 1; i >= 0; i-- {
		conns[i].mu.Unlock()
	}
}

func (b *rdmaBackend) poll(ctx context.Context, conn *rdmaConn, n int) error {
	for i := 0; i < n; i++ {
		if _, err := rdma.PollCompletion(ctx, conn.cq); err != nil {
			return err
		}
	}
	return nil
}

func (b *rdmaBackend) conn(peer int) (*rdmaConn, error) {
	if peer < 0 || peer >= b.size {
		return nil, fmt.Errorf("rank %d out of range for size %d", peer, b.size)
	}
	conn := b.conns[peer]
	if conn == nil {
		return nil, fmt.Errorf("rank %d has no RDMA connection", peer)
	}
	return conn, nil
}

func (b *rdmaBackend) close() error {
	var first error
	for _, conn := range b.conns {
		if conn != nil {
			if err := conn.close(); err != nil && first == nil {
				first = err
			}
		}
	}
	if b.side != nil {
		if err := b.side.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (c *rdmaConn) close() error {
	var first error
	if c.sendMR != nil {
		if err := c.sendMR.Close(); err != nil && first == nil {
			first = err
		}
	}
	if c.recvMR != nil {
		if err := c.recvMR.Close(); err != nil && first == nil {
			first = err
		}
	}
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
	if c.pd != nil {
		if err := c.pd.Close(); err != nil && first == nil {
			first = err
		}
	}
	if c.dev != nil {
		if err := c.dev.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func applyReduction(op reductionOp, dt reduce.DType, dst, src []byte) error {
	switch op {
	case reductionSum:
		return reduce.Sum(dt, dst, src)
	case reductionMax:
		return reduce.Max(dt, dst, src)
	case reductionMin:
		return reduce.Min(dt, dst, src)
	default:
		return fmt.Errorf("unknown reduction op %d", op)
	}
}

func workID(kind, peer int) uint64 {
	return uint64(kind<<16 | peer)
}

func hasConn(conns []*rdmaConn, conn *rdmaConn) bool {
	for _, c := range conns {
		if c == conn {
			return true
		}
	}
	return false
}

func mod(x, n int) int {
	x %= n
	if x < 0 {
		x += n
	}
	return x
}
