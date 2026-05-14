package ipc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"

	"github.com/tmc/gojaccl/internal/allocator"
	"github.com/tmc/gojaccl/internal/jaccld/resource"
)

// Server serves local jaccld control requests.
type Server struct {
	slab      *allocator.Slab
	transport Transport
	resources *resource.Store
}

// Transport performs daemon-owned data movement over registered slab offsets.
type Transport interface {
	Barrier(context.Context) error
	Send(context.Context, int, int64, int64) error
	Recv(context.Context, int, int64, int64) error
}

// MaintenanceTransport performs daemon-owned data-QP maintenance work.
type MaintenanceTransport interface {
	Maintain(context.Context) error
}

// CollectiveTransport performs daemon-owned asynchronous collective work.
type CollectiveTransport interface {
	AllReduce(context.Context, int, int, int64, int64, int64, int64) error
	AllGather(context.Context, int, int64, int64, int64, int64) error
}

// NewServer returns a Server backed by slab.
func NewServer(slab *allocator.Slab, transport Transport) (*Server, error) {
	return NewServerWithResources(slab, transport, nil)
}

// NewServerWithResources returns a Server backed by slab and resources.
func NewServerWithResources(slab *allocator.Slab, transport Transport, resources *resource.Store) (*Server, error) {
	if slab == nil {
		return nil, fmt.Errorf("new ipc server: nil slab")
	}
	if transport == nil {
		transport = disabledTransport{}
	}
	return &Server{slab: slab, transport: transport, resources: resources}, nil
}

// ListenAndServe listens on path and serves until ctx is canceled.
// It sets the socket mode to 0600 after binding. Callers that need to avoid
// the bind-to-chmod window should use a path in an owner-only directory.
func (s *Server) ListenAndServe(ctx context.Context, path string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if path == "" {
		path = DefaultSocket
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("listen ipc: remove stale socket: %w", err)
	}
	addr := net.UnixAddr{Name: path, Net: "unix"}
	ln, err := net.ListenUnix("unix", &addr)
	if err != nil {
		return fmt.Errorf("listen ipc %s: %w", path, err)
	}
	// The final mode is owner-only. A fully atomic owner-only bind requires
	// placing the socket in an owner-only directory.
	if err := os.Chmod(path, 0600); err != nil {
		_ = ln.Close()
		return fmt.Errorf("listen ipc %s: chmod socket: %w", path, err)
	}
	defer os.Remove(path)
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-done:
		}
	}()
	defer close(done)

	for {
		conn, err := ln.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept ipc: %w", err)
		}
		go s.serve(ctx, conn)
	}
}

func (s *Server) serve(ctx context.Context, conn *net.UnixConn) {
	defer conn.Close()
	workCtx, cancelWork := context.WithCancel(ctx)
	leases := make(map[uint64]allocator.Lease)
	sessions := make(map[resource.LeaseID]bool)
	work := make(map[uint64]<-chan error)
	var nextWork uint64
	var wg sync.WaitGroup
	defer func() {
		cancelWork()
		wg.Wait()
		for id := range leases {
			_ = s.slab.Free(id)
		}
		if s.resources != nil {
			for id := range sessions {
				_ = s.resources.Close(context.Background(), id)
			}
		}
	}()
	for {
		var req Request
		if err := readControl(conn, &req); err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			_ = writeControl(conn, Response{Error: err.Error()}, nil)
			continue
		}
		switch req.Op {
		case opAlloc:
			lease, err := s.slab.Alloc(req.Size)
			if err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			leases[lease.ID] = lease
			_ = writeControl(conn, Response{OK: true, Lease: lease}, nil)
		case opFree:
			if _, ok := leases[req.LeaseID]; !ok {
				_ = writeControl(conn, Response{Error: allocator.ErrLeaseNotFound.Error()}, nil)
				continue
			}
			if err := s.slab.Free(req.LeaseID); err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			delete(leases, req.LeaseID)
			_ = writeControl(conn, Response{OK: true}, nil)
		case opSend:
			if err := checkRange(req, leases); err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			if err := s.transport.Send(ctx, req.Peer, req.Offset, req.Length); err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			_ = writeControl(conn, Response{OK: true}, nil)
		case opRecv:
			if err := checkRange(req, leases); err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			if err := s.transport.Recv(ctx, req.Peer, req.Offset, req.Length); err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			_ = writeControl(conn, Response{OK: true}, nil)
		case opBarrier:
			if err := s.transport.Barrier(ctx); err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			_ = writeControl(conn, Response{OK: true}, nil)
		case opMaintain:
			mt, ok := s.transport.(MaintenanceTransport)
			if !ok {
				_ = writeControl(conn, Response{Error: ErrNoTransport.Error()}, nil)
				continue
			}
			if err := mt.Maintain(ctx); err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			_ = writeControl(conn, Response{OK: true}, nil)
		case opMap:
			fd, err := s.slab.FD()
			if err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			_ = writeControl(conn, Response{OK: true, SlabSize: s.slab.Size()}, []int{fd})
		case opStats:
			_ = writeControl(conn, Response{OK: true, Stats: s.slab.Stats()}, nil)
		case opSessionOpen:
			if s.resources == nil {
				_ = writeControl(conn, Response{Error: resource.ErrNotReady.Error()}, nil)
				continue
			}
			lease, err := s.resources.Open(ctx, resource.SessionRequest{
				ClientID:    req.ClientID,
				Peer:        req.SessionPeer,
				Size:        req.Size,
				Deadline:    req.Deadline,
				HeartbeatMR: req.HeartbeatMR,
				Heartbeat:   req.Heartbeat,
			})
			if err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			sessions[lease.ID] = true
			_ = writeControl(conn, Response{OK: true, Session: lease}, nil)
		case opSessionRefresh:
			if s.resources == nil {
				_ = writeControl(conn, Response{Error: resource.ErrNotReady.Error()}, nil)
				continue
			}
			id := resource.LeaseID(req.SessionID)
			if !sessions[id] {
				_ = writeControl(conn, Response{Error: resource.ErrLeaseNotFound.Error()}, nil)
				continue
			}
			lease, err := s.resources.Refresh(id, req.Deadline)
			if err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			_ = writeControl(conn, Response{OK: true, Session: lease}, nil)
		case opSessionLookup:
			if s.resources == nil {
				_ = writeControl(conn, Response{Error: resource.ErrNotReady.Error()}, nil)
				continue
			}
			id := resource.LeaseID(req.SessionID)
			if !sessions[id] {
				_ = writeControl(conn, Response{Error: resource.ErrLeaseNotFound.Error()}, nil)
				continue
			}
			lease, err := s.resources.LookupLive(id)
			if err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			_ = writeControl(conn, Response{OK: true, Session: lease}, nil)
		case opSessionClose:
			if s.resources == nil {
				_ = writeControl(conn, Response{Error: resource.ErrNotReady.Error()}, nil)
				continue
			}
			id := resource.LeaseID(req.SessionID)
			if !sessions[id] {
				_ = writeControl(conn, Response{Error: resource.ErrLeaseNotFound.Error()}, nil)
				continue
			}
			if err := s.resources.Close(ctx, id); err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			delete(sessions, id)
			_ = writeControl(conn, Response{OK: true}, nil)
		case opSessionStats:
			if s.resources == nil {
				_ = writeControl(conn, Response{Error: resource.ErrNotReady.Error()}, nil)
				continue
			}
			_ = writeControl(conn, Response{OK: true, ResourceStats: s.resources.Stats()}, nil)
		case opSubmitReduce:
			ct, ok := s.transport.(CollectiveTransport)
			if !ok {
				_ = writeControl(conn, Response{Error: ErrNoTransport.Error()}, nil)
				continue
			}
			if err := checkAsyncRange(req, leases); err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			nextWork++
			id := nextWork
			work[id] = startWork(&wg, func() error {
				return ct.AllReduce(workCtx, req.ReductionOp, req.DType, req.DstOffset, req.DstLength, req.SrcOffset, req.SrcLength)
			})
			_ = writeControl(conn, Response{OK: true, WorkID: id}, nil)
		case opSubmitGather:
			ct, ok := s.transport.(CollectiveTransport)
			if !ok {
				_ = writeControl(conn, Response{Error: ErrNoTransport.Error()}, nil)
				continue
			}
			if err := checkAsyncRange(req, leases); err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			nextWork++
			id := nextWork
			work[id] = startWork(&wg, func() error {
				return ct.AllGather(workCtx, req.ElementSize, req.DstOffset, req.DstLength, req.SrcOffset, req.SrcLength)
			})
			_ = writeControl(conn, Response{OK: true, WorkID: id}, nil)
		case opWaitWork:
			ch, ok := work[req.WorkID]
			if !ok {
				_ = writeControl(conn, Response{Error: fmt.Sprintf("unknown work id %d", req.WorkID)}, nil)
				continue
			}
			select {
			case err := <-ch:
				delete(work, req.WorkID)
				if err != nil {
					_ = writeControl(conn, Response{OK: true, Ready: true, WorkError: err.Error()}, nil)
					continue
				}
				_ = writeControl(conn, Response{OK: true, Ready: true}, nil)
			default:
				_ = writeControl(conn, Response{OK: true}, nil)
			}
		default:
			_ = writeControl(conn, Response{Error: fmt.Sprintf("unknown op %q", req.Op)}, nil)
		}
	}
}

func startWork(wg *sync.WaitGroup, fn func() error) <-chan error {
	ch := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ch <- fn()
	}()
	return ch
}

type disabledTransport struct{}

func (disabledTransport) Barrier(context.Context) error {
	return ErrNoTransport
}

func (disabledTransport) Send(context.Context, int, int64, int64) error {
	return ErrNoTransport
}

func (disabledTransport) Recv(context.Context, int, int64, int64) error {
	return ErrNoTransport
}

func checkRange(req Request, leases map[uint64]allocator.Lease) error {
	if req.Peer < 0 {
		return fmt.Errorf("peer %d out of range", req.Peer)
	}
	lease, ok := leases[req.LeaseID]
	if !ok {
		return allocator.ErrLeaseNotFound
	}
	if req.Length < 0 {
		return fmt.Errorf("lease %d length %d is negative", req.LeaseID, req.Length)
	}
	end := req.Offset + req.Length
	leaseEnd := lease.Offset + lease.Length
	if end < req.Offset || req.Offset < lease.Offset || end > leaseEnd {
		return fmt.Errorf("range [%d,%d) outside lease %d range [%d,%d)", req.Offset, end, req.LeaseID, lease.Offset, leaseEnd)
	}
	return nil
}

func checkAsyncRange(req Request, leases map[uint64]allocator.Lease) error {
	if req.DstOffset < 0 || req.DstLength < 0 || req.SrcOffset < 0 || req.SrcLength < 0 {
		return fmt.Errorf("async range is invalid")
	}
	if req.DstOffset+req.DstLength < req.DstOffset || req.SrcOffset+req.SrcLength < req.SrcOffset {
		return fmt.Errorf("async range overflows")
	}
	if req.SrcLength == 0 {
		return fmt.Errorf("async source length is zero")
	}
	if req.DstLength < req.SrcLength {
		return fmt.Errorf("async destination length %d, want at least %d", req.DstLength, req.SrcLength)
	}
	if err := checkRange(Request{LeaseID: req.SrcLeaseID, Peer: 0, Offset: req.SrcOffset, Length: req.SrcLength}, leases); err != nil {
		return fmt.Errorf("async source: %w", err)
	}
	if err := checkRange(Request{LeaseID: req.DstLeaseID, Peer: 0, Offset: req.DstOffset, Length: req.DstLength}, leases); err != nil {
		return fmt.Errorf("async destination: %w", err)
	}
	return nil
}

func readControl(conn *net.UnixConn, v any) error {
	var buf [64 << 10]byte
	n, _, _, _, err := conn.ReadMsgUnix(buf[:], nil)
	if err != nil {
		return err
	}
	if n == 0 {
		return io.EOF
	}
	if err := json.Unmarshal(buf[:n], v); err != nil {
		return fmt.Errorf("decode ipc request: %w", err)
	}
	return nil
}

func writeControl(conn *net.UnixConn, resp Response, fds []int) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("encode ipc response: %w", err)
	}
	data = append(data, '\n')
	var oob []byte
	if len(fds) > 0 {
		oob = syscall.UnixRights(fds...)
	}
	if _, _, err := conn.WriteMsgUnix(data, oob, nil); err != nil {
		return fmt.Errorf("write ipc response: %w", err)
	}
	return nil
}
