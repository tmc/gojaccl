package jaccl

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestStreamCopy(t *testing.T) {
	net := newFakeNetwork(2)
	g0 := newFakeGroup(0, 2, net)
	g1 := newFakeGroup(1, 2, net)
	payload := []byte("0123456789abcdef")

	var got bytes.Buffer
	errc := make(chan error, 2)
	go func() {
		w, err := newSendWriter(context.Background(), g0, 1, 5)
		if err == nil {
			_, err = io.Copy(w, bytes.NewReader(payload))
		}
		if closeErr := w.Close(); err == nil {
			err = closeErr
		}
		errc <- err
	}()
	go func() {
		r, err := newRecvReader(context.Background(), g1, 0, 5)
		if err == nil {
			_, err = io.Copy(&got, r)
		}
		if closeErr := r.Close(); err == nil {
			err = closeErr
		}
		errc <- err
	}()
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("copy = %q, want %q", got.Bytes(), payload)
	}
}

func TestStreamReadSmallBuffer(t *testing.T) {
	net := newFakeNetwork(2)
	g0 := newFakeGroup(0, 2, net)
	g1 := newFakeGroup(1, 2, net)

	errc := make(chan error, 1)
	go func() {
		w, err := newSendWriter(context.Background(), g0, 1, 6)
		if err == nil {
			_, err = w.Write([]byte("abcdef"))
		}
		if closeErr := w.Close(); err == nil {
			err = closeErr
		}
		errc <- err
	}()

	r, err := newRecvReader(context.Background(), g1, 0, 6)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	var got []byte
	for {
		n, err := r.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if string(got) != "abcdef" {
		t.Fatalf("read = %q, want abcdef", got)
	}
}

func TestStreamErrors(t *testing.T) {
	t.Run("WriteAfterClose", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		w, err := newSendWriter(context.Background(), g, 1, 4)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		w.ctx = ctx
		_ = w.Close()
		if _, err := w.Write([]byte("x")); !errors.Is(err, ErrClosed) {
			t.Fatalf("Write after Close = %v, want ErrClosed", err)
		}
	})
	t.Run("InvalidRank", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		if _, err := g.NewSendWriter(context.Background(), 2); err == nil {
			t.Fatal("NewSendWriter invalid rank = nil")
		}
		if _, err := g.NewRecvReader(context.Background(), -1); err == nil {
			t.Fatal("NewRecvReader invalid rank = nil")
		}
	})
	t.Run("BadChunkSize", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		if _, err := newSendWriter(context.Background(), g, 1, 0); err == nil || !strings.Contains(err.Error(), "not positive") {
			t.Fatalf("newSendWriter bad chunk = %v, want not positive", err)
		}
		if _, err := newRecvReader(context.Background(), g, 1, -1); err == nil || !strings.Contains(err.Error(), "not positive") {
			t.Fatalf("newRecvReader bad chunk = %v, want not positive", err)
		}
	})
	t.Run("ReadDeadline", func(t *testing.T) {
		g := newFakeGroup(0, 2, newFakeNetwork(2))
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		r, err := newRecvReader(ctx, g, 1, 4)
		if err != nil {
			t.Fatal(err)
		}
		_, err = r.Read(make([]byte, 1))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Read deadline = %v, want deadline", err)
		}
	})
}
