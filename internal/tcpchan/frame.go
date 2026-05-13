package tcpchan

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const maxFrameSize = 8 << 10

var (
	ErrFrameTooLarge  = errors.New("tcpchan: frame exceeds maximum size")
	ErrMalformedFrame = errors.New("tcpchan: malformed frame")
)

func WriteFrame(w io.Writer, payload []byte) error {
	if len(payload) > maxFrameSize {
		return ErrFrameTooLarge
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("tcpchan: write frame header: %w", err)
	}
	if len(payload) == 0 {
		return nil
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("tcpchan: write frame payload: %w", err)
	}
	return nil
}

func ReadFrame(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: %v", ErrMalformedFrame, err)
		}
		return nil, fmt.Errorf("tcpchan: read frame header: %w", err)
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxFrameSize {
		return nil, ErrFrameTooLarge
	}
	payload := make([]byte, int(n))
	if n == 0 {
		return payload, nil
	}
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: %v", ErrMalformedFrame, err)
		}
		return nil, fmt.Errorf("tcpchan: read frame payload: %w", err)
	}
	return payload, nil
}
