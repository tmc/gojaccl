package jaccl

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
)

// DefaultStreamChunkSize is the largest payload sent in one stream frame.
const DefaultStreamChunkSize = 1 << 20

// SendWriter streams bytes from this group to one peer.
//
// Close must be called to send end-of-stream to the peer.
type SendWriter struct {
	g      *Group
	ctx    context.Context
	dst    int
	chunk  int
	closed bool
}

// RecvReader streams bytes from one peer to this group.
type RecvReader struct {
	g       *Group
	ctx     context.Context
	src     int
	max     int
	pending []byte
	eof     bool
	closed  bool
}

var (
	_ io.WriteCloser = (*SendWriter)(nil)
	_ io.ReaderFrom  = (*SendWriter)(nil)
	_ io.ReadCloser  = (*RecvReader)(nil)
	_ io.WriterTo    = (*RecvReader)(nil)
)

// NewSendWriter returns a writer that sends bytes to dst.
func (g *Group) NewSendWriter(ctx context.Context, dst int) (*SendWriter, error) {
	return newSendWriter(ctx, g, dst, DefaultStreamChunkSize)
}

func newSendWriter(ctx context.Context, g *Group, dst, chunk int) (*SendWriter, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := g.checkRank("new send writer", dst); err != nil {
		return nil, err
	}
	if chunk <= 0 {
		return nil, wrapError(g.rank, "new send writer", fmt.Errorf("chunk size %d is not positive", chunk))
	}
	return &SendWriter{g: g, ctx: ctx, dst: dst, chunk: chunk}, nil
}

// Write sends p to the peer.
func (w *SendWriter) Write(p []byte) (int, error) {
	if w == nil || w.closed {
		return 0, ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	written := 0
	for written < len(p) {
		n := min(w.chunk, len(p)-written)
		if err := w.g.sendStreamFrame(w.ctx, w.dst, p[written:written+n]); err != nil {
			return written, err
		}
		written += n
	}
	return written, nil
}

// ReadFrom reads from r and sends the bytes to the peer.
func (w *SendWriter) ReadFrom(r io.Reader) (int64, error) {
	if w == nil || w.closed {
		return 0, ErrClosed
	}
	buf := make([]byte, w.chunk)
	var total int64
	for {
		nr, er := r.Read(buf)
		if nr > 0 {
			nw, ew := w.Write(buf[:nr])
			total += int64(nw)
			if ew != nil {
				return total, ew
			}
			if nw != nr {
				return total, io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				return total, nil
			}
			return total, er
		}
	}
}

// Close sends end-of-stream to the peer. It is safe to call Close more than
// once.
func (w *SendWriter) Close() error {
	if w == nil || w.closed {
		return nil
	}
	w.closed = true
	return w.g.sendStreamFrame(w.ctx, w.dst, nil)
}

// NewRecvReader returns a reader that receives bytes from src.
func (g *Group) NewRecvReader(ctx context.Context, src int) (*RecvReader, error) {
	return newRecvReader(ctx, g, src, DefaultStreamChunkSize)
}

func newRecvReader(ctx context.Context, g *Group, src, max int) (*RecvReader, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := g.checkRank("new recv reader", src); err != nil {
		return nil, err
	}
	if max <= 0 {
		return nil, wrapError(g.rank, "new recv reader", fmt.Errorf("frame size %d is not positive", max))
	}
	return &RecvReader{g: g, ctx: ctx, src: src, max: max}, nil
}

// Read receives bytes from the peer.
func (r *RecvReader) Read(p []byte) (int, error) {
	if r == nil || r.closed {
		return 0, ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.pending) > 0 {
		return r.readPending(p), nil
	}
	if r.eof {
		return 0, io.EOF
	}
	frame, err := r.g.recvStreamFrame(r.ctx, r.src, r.max)
	if err != nil {
		return 0, err
	}
	if frame == nil {
		r.eof = true
		return 0, io.EOF
	}
	n := copy(p, frame)
	if n < len(frame) {
		r.pending = frame[n:]
	}
	return n, nil
}

// WriteTo receives bytes from the peer and writes them to w.
func (r *RecvReader) WriteTo(w io.Writer) (int64, error) {
	if r == nil || r.closed {
		return 0, ErrClosed
	}
	var total int64
	if len(r.pending) > 0 {
		n, err := writeAll(w, r.pending)
		total += n
		r.pending = nil
		if err != nil {
			return total, err
		}
	}
	for !r.eof {
		frame, err := r.g.recvStreamFrame(r.ctx, r.src, r.max)
		if err != nil {
			return total, err
		}
		if frame == nil {
			r.eof = true
			return total, nil
		}
		n, err := writeAll(w, frame)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// Close releases the reader. It does not notify the peer.
func (r *RecvReader) Close() error {
	if r == nil {
		return nil
	}
	r.closed = true
	r.pending = nil
	return nil
}

func (r *RecvReader) readPending(p []byte) int {
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	if len(r.pending) == 0 {
		r.pending = nil
	}
	return n
}

func (g *Group) sendStreamFrame(ctx context.Context, dst int, payload []byte) error {
	if err := g.checkRank("stream send", dst); err != nil {
		return err
	}
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], uint64(len(payload)))
	return g.do(ctx, "stream send", func(b backend) error {
		if err := b.send(ctx, dst, header[:]); err != nil {
			return err
		}
		if len(payload) == 0 {
			return nil
		}
		return b.send(ctx, dst, payload)
	})
}

func (g *Group) recvStreamFrame(ctx context.Context, src, max int) ([]byte, error) {
	if err := g.checkRank("stream recv", src); err != nil {
		return nil, err
	}
	var frame []byte
	var header [8]byte
	err := g.do(ctx, "stream recv", func(b backend) error {
		if err := b.recv(ctx, src, header[:]); err != nil {
			return err
		}
		n := binary.BigEndian.Uint64(header[:])
		if n == 0 {
			return nil
		}
		if n > uint64(max) {
			return fmt.Errorf("stream frame length %d exceeds limit %d", n, max)
		}
		frame = make([]byte, int(n))
		return b.recv(ctx, src, frame)
	})
	if err != nil {
		return nil, err
	}
	return frame, nil
}

func writeAll(w io.Writer, p []byte) (int64, error) {
	var total int64
	for len(p) > 0 {
		n, err := w.Write(p)
		total += int64(n)
		p = p[n:]
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}
