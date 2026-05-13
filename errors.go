package jaccl

import (
	"errors"
	"fmt"
)

var (
	// ErrClosed reports an operation attempted after a group has been closed.
	ErrClosed = errors.New("group closed")

	// ErrBusy reports that a group already has an active operation.
	ErrBusy = errors.New("operation in progress")

	// ErrNotImplemented reports an accepted API path whose backend is not built.
	ErrNotImplemented = errors.New("not implemented")
)

type opError struct {
	rank int
	op   string
	err  error
}

func (e *opError) Error() string {
	if e.rank >= 0 {
		return fmt.Sprintf("jaccl: rank %d %s: %v", e.rank, e.op, e.err)
	}
	return fmt.Sprintf("jaccl: %s: %v", e.op, e.err)
}

func (e *opError) Unwrap() error {
	return e.err
}

func wrapError(rank int, op string, err error) error {
	if err == nil {
		return nil
	}
	if op == "" {
		op = "operation"
	}
	return &opError{rank: rank, op: op, err: err}
}
