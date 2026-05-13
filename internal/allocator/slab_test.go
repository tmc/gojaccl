package allocator

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestSlabAlloc(t *testing.T) {
	s, err := NewSlab(t.TempDir(), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	a, err := s.Alloc(4)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Alloc(8)
	if err != nil {
		t.Fatal(err)
	}
	if a.Offset != 0 || a.Length != 4 {
		t.Fatalf("first lease = %+v, want offset 0 length 4", a)
	}
	if b.Offset != 4 || b.Length != 8 {
		t.Fatalf("second lease = %+v, want offset 4 length 8", b)
	}
	st := s.Stats()
	if st.Used != 12 || st.Free != 4 || st.Leases != 2 {
		t.Fatalf("stats = %+v, want used 12 free 4 leases 2", st)
	}
}

func TestSlabFreeCoalesces(t *testing.T) {
	s, err := NewSlab(t.TempDir(), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	a, _ := s.Alloc(4)
	b, _ := s.Alloc(4)
	c, _ := s.Alloc(4)
	if err := s.Free(b.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Free(a.ID); err != nil {
		t.Fatal(err)
	}
	d, err := s.Alloc(8)
	if err != nil {
		t.Fatal(err)
	}
	if d.Offset != 0 {
		t.Fatalf("coalesced lease offset = %d, want 0", d.Offset)
	}
	if err := s.Free(c.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Free(d.ID); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	if st.Free != 16 || st.FreeRanges != 1 || st.Leases != 0 {
		t.Fatalf("stats after free = %+v, want one free range", st)
	}
}

func TestSlabAllocRejectsBadSizes(t *testing.T) {
	if _, err := NewSlab(t.TempDir(), 0); err == nil {
		t.Fatal("NewSlab(0) = nil")
	}
	s, err := NewSlab(t.TempDir(), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, n := range []int64{0, -1} {
		if _, err := s.Alloc(n); err == nil {
			t.Fatalf("Alloc(%d) = nil", n)
		}
	}
	if _, err := s.Alloc(9); !errors.Is(err, ErrNoMemory) {
		t.Fatalf("Alloc(9) = %v, want ErrNoMemory", err)
	}
}

func TestSlabFreeRejectsUnknownLease(t *testing.T) {
	s, err := NewSlab(t.TempDir(), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Free(99); !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("Free(99) = %v, want ErrLeaseNotFound", err)
	}
}

func TestSlabSharedMappingAndClose(t *testing.T) {
	s, err := NewSlab(t.TempDir(), 8)
	if err != nil {
		t.Fatal(err)
	}
	path := s.Path()
	fd, err := s.FD()
	if err != nil {
		t.Fatal(err)
	}
	if fd < 0 {
		t.Fatalf("FD = %d, want non-negative", fd)
	}
	s.Bytes()[3] = 42
	buf := make([]byte, 4)
	if _, err := syscall.Pread(fd, buf, 0); err != nil {
		t.Fatal(err)
	}
	if buf[3] != 42 {
		t.Fatalf("file byte = %d, want 42", buf[3])
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat before Close = %v, want unlinked backing file", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FD(); !errors.Is(err, ErrClosed) {
		t.Fatalf("FD after Close = %v, want ErrClosed", err)
	}
}
