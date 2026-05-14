package ipc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/tmc/gojaccl/internal/allocator"
	"github.com/tmc/gojaccl/internal/jaccld/resource"
)

// Client is a local connection to jaccld.
type Client struct {
	mu   sync.Mutex
	conn *net.UnixConn
}

// Mapping is a local mmap of the jaccld slab.
type Mapping struct {
	Data []byte

	once sync.Once
	fd   int
}

// Dial connects to a jaccld Unix-domain socket.
func Dial(ctx context.Context, path string) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if path == "" {
		path = DefaultSocket
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("dial jaccld %s: %w", path, err)
	}
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("dial jaccld %s: not a Unix connection", path)
	}
	return &Client{conn: uc}, nil
}

// Alloc requests n bytes from the daemon slab.
func (c *Client) Alloc(ctx context.Context, n int64) (allocator.Lease, error) {
	resp, fds, err := c.do(ctx, Request{Op: opAlloc, Size: n})
	closeFDs(fds)
	if err != nil {
		return allocator.Lease{}, err
	}
	return resp.Lease, nil
}

// Free releases id.
func (c *Client) Free(ctx context.Context, id uint64) error {
	_, fds, err := c.do(ctx, Request{Op: opFree, LeaseID: id})
	closeFDs(fds)
	return err
}

// Barrier waits until the daemon transport reaches a barrier.
func (c *Client) Barrier(ctx context.Context) error {
	_, fds, err := c.do(ctx, Request{Op: opBarrier})
	closeFDs(fds)
	return err
}

// Maintain asks the daemon transport to run a maintenance operation.
func (c *Client) Maintain(ctx context.Context) error {
	_, fds, err := c.do(ctx, Request{Op: opMaintain})
	closeFDs(fds)
	return err
}

// Send asks the daemon to send a slab range to peer.
//
// The lease value may describe a subrange of the original allocation by keeping
// the same ID and narrowing Offset and Length. The server validates the range
// against the original lease bounds.
func (c *Client) Send(ctx context.Context, peer int, lease allocator.Lease) error {
	_, fds, err := c.do(ctx, Request{
		Op:      opSend,
		LeaseID: lease.ID,
		Peer:    peer,
		Offset:  lease.Offset,
		Length:  lease.Length,
	})
	closeFDs(fds)
	return err
}

// Recv asks the daemon to receive a slab range from peer.
//
// The lease value may describe a subrange of the original allocation by keeping
// the same ID and narrowing Offset and Length. The server validates the range
// against the original lease bounds.
func (c *Client) Recv(ctx context.Context, peer int, lease allocator.Lease) error {
	_, fds, err := c.do(ctx, Request{
		Op:      opRecv,
		LeaseID: lease.ID,
		Peer:    peer,
		Offset:  lease.Offset,
		Length:  lease.Length,
	})
	closeFDs(fds)
	return err
}

// SubmitReduce starts an asynchronous daemon reduction over slab leases.
func (c *Client) SubmitReduce(ctx context.Context, op int, dt int, dst, src allocator.Lease) (uint64, error) {
	resp, fds, err := c.do(ctx, Request{
		Op:          opSubmitReduce,
		ReductionOp: op,
		DType:       dt,
		DstLeaseID:  dst.ID,
		DstOffset:   dst.Offset,
		DstLength:   dst.Length,
		SrcLeaseID:  src.ID,
		SrcOffset:   src.Offset,
		SrcLength:   src.Length,
	})
	closeFDs(fds)
	if err != nil {
		return 0, err
	}
	return resp.WorkID, nil
}

// SubmitGather starts an asynchronous daemon all-gather over slab leases.
func (c *Client) SubmitGather(ctx context.Context, elemSize int, dst, src allocator.Lease) (uint64, error) {
	resp, fds, err := c.do(ctx, Request{
		Op:          opSubmitGather,
		ElementSize: elemSize,
		DstLeaseID:  dst.ID,
		DstOffset:   dst.Offset,
		DstLength:   dst.Length,
		SrcLeaseID:  src.ID,
		SrcOffset:   src.Offset,
		SrcLength:   src.Length,
	})
	closeFDs(fds)
	if err != nil {
		return 0, err
	}
	return resp.WorkID, nil
}

// WaitWork waits for asynchronous daemon work to finish.
func (c *Client) WaitWork(ctx context.Context, id uint64) error {
	backoff := time.Millisecond
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		resp, fds, err := c.do(context.Background(), Request{Op: opWaitWork, WorkID: id})
		closeFDs(fds)
		if err != nil {
			return err
		}
		if resp.Ready {
			if resp.WorkError != "" {
				return fmt.Errorf("%s", resp.WorkError)
			}
			return nil
		}
		if timer == nil {
			timer = time.NewTimer(backoff)
		} else {
			timer.Reset(backoff)
		}
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
		if backoff < 50*time.Millisecond {
			backoff *= 2
		}
	}
}

// Stats returns daemon slab statistics.
func (c *Client) Stats(ctx context.Context) (allocator.Stats, error) {
	resp, fds, err := c.do(ctx, Request{Op: opStats})
	closeFDs(fds)
	if err != nil {
		return allocator.Stats{}, err
	}
	return resp.Stats, nil
}

// OpenSession requests a daemon resource session lease.
func (c *Client) OpenSession(ctx context.Context, req resource.SessionRequest) (resource.SessionLease, error) {
	resp, fds, err := c.do(ctx, Request{
		Op:          opSessionOpen,
		ClientID:    req.ClientID,
		SessionPeer: req.Peer,
		Size:        req.Size,
		Deadline:    req.Deadline,
		HeartbeatMR: req.HeartbeatMR,
		Heartbeat:   req.Heartbeat,
	})
	closeFDs(fds)
	if err != nil {
		return resource.SessionLease{}, err
	}
	return resp.Session, nil
}

// RefreshSession extends a resource session lease deadline.
func (c *Client) RefreshSession(ctx context.Context, id resource.LeaseID, deadline time.Time) (resource.SessionLease, error) {
	resp, fds, err := c.do(ctx, Request{Op: opSessionRefresh, SessionID: uint64(id), Deadline: deadline})
	closeFDs(fds)
	if err != nil {
		return resource.SessionLease{}, err
	}
	return resp.Session, nil
}

// LookupSession returns current metadata for a live resource session lease.
func (c *Client) LookupSession(ctx context.Context, id resource.LeaseID) (resource.SessionLease, error) {
	resp, fds, err := c.do(ctx, Request{Op: opSessionLookup, SessionID: uint64(id)})
	closeFDs(fds)
	if err != nil {
		return resource.SessionLease{}, err
	}
	return resp.Session, nil
}

// CloseSession releases a daemon resource session lease.
func (c *Client) CloseSession(ctx context.Context, id resource.LeaseID) error {
	_, fds, err := c.do(ctx, Request{Op: opSessionClose, SessionID: uint64(id)})
	closeFDs(fds)
	return err
}

// ResourceStats returns daemon resource store statistics.
func (c *Client) ResourceStats(ctx context.Context) (resource.Stats, error) {
	resp, fds, err := c.do(ctx, Request{Op: opSessionStats})
	closeFDs(fds)
	if err != nil {
		return resource.Stats{}, err
	}
	return resp.ResourceStats, nil
}

// Map maps the daemon slab into this process.
func (c *Client) Map(ctx context.Context) (*Mapping, error) {
	resp, fds, err := c.do(ctx, Request{Op: opMap})
	if err != nil {
		closeFDs(fds)
		return nil, err
	}
	if len(fds) != 1 {
		closeFDs(fds)
		return nil, fmt.Errorf("map jaccld slab: got %d file descriptors, want 1", len(fds))
	}
	if resp.SlabSize <= 0 || int64(int(resp.SlabSize)) != resp.SlabSize {
		closeFDs(fds)
		return nil, fmt.Errorf("map jaccld slab: invalid size %d", resp.SlabSize)
	}
	data, err := syscall.Mmap(fds[0], 0, int(resp.SlabSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		closeFDs(fds)
		return nil, fmt.Errorf("map jaccld slab: mmap: %w", err)
	}
	return &Mapping{Data: data, fd: fds[0]}, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Close unmaps the slab and closes the received file descriptor.
func (m *Mapping) Close() error {
	if m == nil {
		return nil
	}
	var first error
	m.once.Do(func() {
		if m.Data != nil {
			if err := syscall.Munmap(m.Data); err != nil {
				first = fmt.Errorf("close mapping: munmap: %w", err)
			}
			m.Data = nil
		}
		if m.fd >= 0 {
			if err := syscall.Close(m.fd); err != nil && first == nil {
				first = fmt.Errorf("close mapping: fd: %w", err)
			}
			m.fd = -1
		}
	})
	return first
}

func (c *Client) do(ctx context.Context, req Request) (Response, []int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Response{}, nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return Response{}, nil, fmt.Errorf("jaccld client closed")
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(timeZero)
	}
	data, err := json.Marshal(req)
	if err != nil {
		return Response{}, nil, fmt.Errorf("encode ipc request: %w", err)
	}
	data = append(data, '\n')
	if _, _, err := c.conn.WriteMsgUnix(data, nil, nil); err != nil {
		return Response{}, nil, fmt.Errorf("write ipc request: %w", err)
	}
	var resp Response
	fds, err := readResponse(c.conn, &resp)
	if err != nil {
		return Response{}, fds, err
	}
	if err := resp.err(); err != nil {
		return resp, fds, err
	}
	return resp, fds, nil
}

func readResponse(conn *net.UnixConn, resp *Response) ([]int, error) {
	buf := make([]byte, 64<<10)
	oob := make([]byte, syscall.CmsgSpace(4*4))
	n, oobn, flags, _, err := conn.ReadMsgUnix(buf, oob)
	if err != nil {
		return nil, fmt.Errorf("read ipc response: %w", err)
	}
	if flags&syscall.MSG_CTRUNC != 0 {
		return nil, fmt.Errorf("read ipc response: control message truncated")
	}
	if err := json.Unmarshal(buf[:n], resp); err != nil {
		return nil, fmt.Errorf("decode ipc response: %w", err)
	}
	if oobn == 0 {
		return nil, nil
	}
	msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, fmt.Errorf("parse ipc control message: %w", err)
	}
	var fds []int
	for _, msg := range msgs {
		rights, err := syscall.ParseUnixRights(&msg)
		if err != nil {
			closeFDs(fds)
			return nil, fmt.Errorf("parse ipc file descriptors: %w", err)
		}
		fds = append(fds, rights...)
	}
	return fds, nil
}

func closeFDs(fds []int) {
	for _, fd := range fds {
		_ = syscall.Close(fd)
	}
}

var timeZero = time.Time{}
