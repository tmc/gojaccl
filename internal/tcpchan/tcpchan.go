package tcpchan

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// Channel is the TCP side channel used to exchange rank metadata.
type Channel struct {
	rank int
	size int

	listener net.Listener
	peers    []net.Conn
	once     sync.Once
}

type hello struct {
	Rank int
	Size int
}

// New connects all ranks to the rank-zero coordinator.
func New(ctx context.Context, rank, size int, coordinator string) (*Channel, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if size < 1 {
		return nil, fmt.Errorf("tcpchan: size %d must be positive", size)
	}
	if rank < 0 || rank >= size {
		return nil, fmt.Errorf("tcpchan: rank %d out of range for size %d", rank, size)
	}
	c := &Channel{rank: rank, size: size, peers: make([]net.Conn, size)}
	if rank == 0 {
		if err := c.listen(ctx, coordinator); err != nil {
			_ = c.Close()
			return nil, err
		}
		return c, nil
	}
	if err := c.dial(ctx, coordinator); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Channel) listen(ctx context.Context, coordinator string) error {
	ln, err := net.Listen("tcp", coordinator)
	if err != nil {
		return fmt.Errorf("tcpchan: listen %s: %w", coordinator, err)
	}
	c.listener = ln
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-done:
		}
	}()
	defer close(done)

	for accepted := 1; accepted < c.size; {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("tcpchan: accept: %w", err)
		}
		msg, err := readJSON[hello](ctx, conn)
		if err != nil {
			_ = conn.Close()
			return err
		}
		if msg.Size != c.size {
			_ = conn.Close()
			return fmt.Errorf("tcpchan: peer rank %d size %d, want %d", msg.Rank, msg.Size, c.size)
		}
		if msg.Rank <= 0 || msg.Rank >= c.size {
			_ = conn.Close()
			return fmt.Errorf("tcpchan: peer rank %d out of range", msg.Rank)
		}
		if c.peers[msg.Rank] != nil {
			_ = conn.Close()
			return fmt.Errorf("tcpchan: duplicate rank %d", msg.Rank)
		}
		c.peers[msg.Rank] = conn
		if err := writeJSON(ctx, conn, hello{Rank: c.rank, Size: c.size}); err != nil {
			return err
		}
		accepted++
	}
	return nil
}

func (c *Channel) dial(ctx context.Context, coordinator string) error {
	d := net.Dialer{}
	var last error
	for {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return fmt.Errorf("tcpchan: dial %s: %w", coordinator, err)
			}
			return err
		}
		conn, err := d.DialContext(ctx, "tcp", coordinator)
		if err == nil {
			if err := writeJSON(ctx, conn, hello{Rank: c.rank, Size: c.size}); err != nil {
				_ = conn.Close()
				return err
			}
			ack, err := readJSON[hello](ctx, conn)
			if err != nil {
				_ = conn.Close()
				return err
			}
			if ack.Rank != 0 || ack.Size != c.size {
				_ = conn.Close()
				return fmt.Errorf("tcpchan: bad coordinator ack rank=%d size=%d", ack.Rank, ack.Size)
			}
			c.peers[0] = conn
			return nil
		}
		last = err
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("tcpchan: dial %s: %w", coordinator, ctx.Err())
		}
	}
}

// AllGather exchanges one opaque metadata value per rank.
func (c *Channel) AllGather(ctx context.Context, local []byte) ([][]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("tcpchan: closed")
	}
	if len(local) > maxFrameSize {
		return nil, ErrFrameTooLarge
	}
	if c.rank == 0 {
		values := make([][]byte, c.size)
		values[0] = append([]byte(nil), local...)
		for rank := 1; rank < c.size; rank++ {
			payload, err := readFrame(ctx, c.peers[rank])
			if err != nil {
				return nil, err
			}
			values[rank] = payload
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return nil, fmt.Errorf("tcpchan: marshal allgather: %w", err)
		}
		for rank := 1; rank < c.size; rank++ {
			if err := writeFrame(ctx, c.peers[rank], encoded); err != nil {
				return nil, err
			}
		}
		return values, nil
	}
	if err := writeFrame(ctx, c.peers[0], local); err != nil {
		return nil, err
	}
	payload, err := readFrame(ctx, c.peers[0])
	if err != nil {
		return nil, err
	}
	var values [][]byte
	if err := json.Unmarshal(payload, &values); err != nil {
		return nil, fmt.Errorf("tcpchan: decode allgather: %w", err)
	}
	if len(values) != c.size {
		return nil, fmt.Errorf("tcpchan: gathered %d values, want %d", len(values), c.size)
	}
	return values, nil
}

// Barrier waits for all ranks to arrive.
func (c *Channel) Barrier(ctx context.Context) error {
	_, err := c.AllGather(ctx, nil)
	return err
}

func (c *Channel) Close() error {
	if c == nil {
		return nil
	}
	c.once.Do(func() {
		if c.listener != nil {
			_ = c.listener.Close()
		}
		for _, p := range c.peers {
			if p != nil {
				_ = p.Close()
			}
		}
	})
	return nil
}

func writeJSON[T any](ctx context.Context, conn net.Conn, v T) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeFrame(ctx, conn, data)
}

func readJSON[T any](ctx context.Context, conn net.Conn) (T, error) {
	var v T
	data, err := readFrame(ctx, conn)
	if err != nil {
		return v, err
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return v, fmt.Errorf("tcpchan: decode frame: %w", err)
	}
	return v, nil
}

func writeFrame(ctx context.Context, conn net.Conn, data []byte) error {
	if err := withContext(ctx, conn, func() error { return WriteFrame(conn, data) }); err != nil {
		return err
	}
	return nil
}

func readFrame(ctx context.Context, conn net.Conn) ([]byte, error) {
	var data []byte
	err := withContext(ctx, conn, func() error {
		var err error
		data, err = ReadFrame(conn)
		return err
	})
	if err != nil {
		return nil, err
	}
	return data, nil
}

func withContext(ctx context.Context, conn net.Conn, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetDeadline(time.Now())
		case <-done:
		}
	}()
	err := fn()
	close(done)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil && hasDeadline && !time.Now().Before(deadline) {
		<-ctx.Done()
		return ctx.Err()
	}
	return err
}
