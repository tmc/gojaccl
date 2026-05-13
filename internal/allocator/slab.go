// Package allocator provides the jaccld shared-memory slab allocator.
package allocator

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
)

// Errors reported by Slab methods.
var (
	ErrClosed        = errors.New("slab closed")
	ErrNoMemory      = errors.New("slab out of memory")
	ErrLeaseNotFound = errors.New("lease not found")
)

// Lease is a logical byte range in a slab.
type Lease struct {
	ID     uint64 `json:"id"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

// Stats reports current slab use.
type Stats struct {
	Size       int64 `json:"size"`
	Used       int64 `json:"used"`
	Free       int64 `json:"free"`
	Leases     int   `json:"leases"`
	FreeRanges int   `json:"free_ranges"`
}

type span struct {
	off int64
	n   int64
}

// Slab is a fixed-size, shared-memory allocation pool.
type Slab struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	data   []byte
	size   int64
	nextID uint64
	free   []span
	leases map[uint64]span
	closed bool
}

// NewSlab creates a file-backed, MAP_SHARED slab of size bytes.
func NewSlab(dir string, size int64) (*Slab, error) {
	if size <= 0 {
		return nil, fmt.Errorf("new slab: size %d must be positive", size)
	}
	if int64(int(size)) != size {
		return nil, fmt.Errorf("new slab: size %d overflows int", size)
	}
	f, err := os.CreateTemp(dir, "jaccld-slab-*")
	if err != nil {
		return nil, fmt.Errorf("new slab: create file: %w", err)
	}
	ok := false
	defer func() {
		if ok {
			return
		}
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()
	if err := f.Truncate(size); err != nil {
		return nil, fmt.Errorf("new slab: truncate: %w", err)
	}
	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("new slab: mmap: %w", err)
	}
	if err := os.Remove(f.Name()); err != nil {
		_ = syscall.Munmap(data)
		return nil, fmt.Errorf("new slab: unlink: %w", err)
	}
	ok = true
	return &Slab{
		file:   f,
		path:   f.Name(),
		data:   data,
		size:   size,
		free:   []span{{n: size}},
		leases: make(map[uint64]span),
	}, nil
}

// Alloc reserves n bytes and returns the resulting lease.
func (s *Slab) Alloc(n int64) (Lease, error) {
	if n <= 0 {
		return Lease{}, fmt.Errorf("alloc slab: size %d must be positive", n)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Lease{}, ErrClosed
	}
	for i, r := range s.free {
		if r.n < n {
			continue
		}
		lease := Lease{
			ID:     s.next(),
			Offset: r.off,
			Length: n,
		}
		s.leases[lease.ID] = span{off: r.off, n: n}
		if r.n == n {
			s.free = append(s.free[:i], s.free[i+1:]...)
			return lease, nil
		}
		s.free[i].off += n
		s.free[i].n -= n
		return lease, nil
	}
	return Lease{}, ErrNoMemory
}

// Free releases a lease by ID.
func (s *Slab) Free(id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	r, ok := s.leases[id]
	if !ok {
		return ErrLeaseNotFound
	}
	delete(s.leases, id)
	s.insertFree(r)
	return nil
}

// Bytes returns the mmap-backed slab bytes.
func (s *Slab) Bytes() []byte {
	if s == nil {
		return nil
	}
	return s.data
}

// FD returns the file descriptor backing the slab.
func (s *Slab) FD() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return -1, ErrClosed
	}
	return int(s.file.Fd()), nil
}

// Path returns the path of the temporary file backing the slab.
func (s *Slab) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// Size reports the slab size in bytes.
func (s *Slab) Size() int64 {
	if s == nil {
		return 0
	}
	return s.size
}

// Stats reports current allocator use.
func (s *Slab) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var free int64
	for _, r := range s.free {
		free += r.n
	}
	return Stats{
		Size:       s.size,
		Used:       s.size - free,
		Free:       free,
		Leases:     len(s.leases),
		FreeRanges: len(s.free),
	}
}

// Close unmaps and removes the slab file. It is safe to call Close more than once.
func (s *Slab) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	var first error
	if s.data != nil {
		if err := syscall.Munmap(s.data); err != nil {
			first = fmt.Errorf("close slab: munmap: %w", err)
		}
		s.data = nil
	}
	if s.file != nil {
		if err := s.file.Close(); err != nil && first == nil {
			first = fmt.Errorf("close slab: file: %w", err)
		}
		s.file = nil
	}
	if s.path != "" {
		if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) && first == nil {
			first = fmt.Errorf("close slab: remove: %w", err)
		}
	}
	s.closed = true
	return first
}

func (s *Slab) next() uint64 {
	s.nextID++
	return s.nextID
}

func (s *Slab) insertFree(r span) {
	i := 0
	for i < len(s.free) && s.free[i].off < r.off {
		i++
	}
	s.free = append(s.free, span{})
	copy(s.free[i+1:], s.free[i:])
	s.free[i] = r
	s.coalesceFree()
}

func (s *Slab) coalesceFree() {
	if len(s.free) < 2 {
		return
	}
	out := s.free[:1]
	for _, r := range s.free[1:] {
		last := &out[len(out)-1]
		if last.off+last.n == r.off {
			last.n += r.n
			continue
		}
		out = append(out, r)
	}
	s.free = out
}
