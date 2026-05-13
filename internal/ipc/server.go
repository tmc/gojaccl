package ipc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"

	"github.com/tmc/gojaccl/internal/allocator"
)

// Server serves local jaccld control requests.
type Server struct {
	slab *allocator.Slab
}

// NewServer returns a Server backed by slab.
func NewServer(slab *allocator.Slab) (*Server, error) {
	if slab == nil {
		return nil, fmt.Errorf("new ipc server: nil slab")
	}
	return &Server{slab: slab}, nil
}

// ListenAndServe listens on path and serves until ctx is canceled.
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
		go s.serve(conn)
	}
}

func (s *Server) serve(conn *net.UnixConn) {
	defer conn.Close()
	leases := make(map[uint64]struct{})
	defer func() {
		for id := range leases {
			_ = s.slab.Free(id)
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
			leases[lease.ID] = struct{}{}
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
		case opMap:
			fd, err := s.slab.FD()
			if err != nil {
				_ = writeControl(conn, Response{Error: err.Error()}, nil)
				continue
			}
			_ = writeControl(conn, Response{OK: true, SlabSize: s.slab.Size()}, []int{fd})
		case opStats:
			_ = writeControl(conn, Response{OK: true, Stats: s.slab.Stats()}, nil)
		default:
			_ = writeControl(conn, Response{Error: fmt.Sprintf("unknown op %q", req.Op)}, nil)
		}
	}
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
