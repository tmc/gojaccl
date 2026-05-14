package keepalive

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTrackerBeatsIdleRoutes(t *testing.T) {
	now := time.Unix(100, 0)
	tr, err := New(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tr.SetNow(func() time.Time { return now })
	var beats int
	if err := tr.Add("qp1", SenderFunc(func(context.Context) error {
		beats++
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := tr.BeatIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if beats != 0 {
		t.Fatalf("beats before idle = %d, want 0", beats)
	}
	now = now.Add(time.Minute)
	if err := tr.BeatIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if beats != 1 {
		t.Fatalf("beats after idle = %d, want 1", beats)
	}
	st, ok := tr.Status("qp1")
	if !ok {
		t.Fatal("missing status")
	}
	if !st.Healthy || st.LastError != "" {
		t.Fatalf("status = %+v, want healthy", st)
	}
	if st.Beats != 1 || st.Errors != 0 {
		t.Fatalf("status counters = %+v, want one beat no errors", st)
	}
	if st.LastActivity != now || st.LastBeat != now {
		t.Fatalf("status times = %+v, want %s", st, now)
	}
}

func TestTrackerTouchDefersHeartbeat(t *testing.T) {
	now := time.Unix(100, 0)
	tr, err := New(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tr.SetNow(func() time.Time { return now })
	var beats int
	if err := tr.Add("qp1", SenderFunc(func(context.Context) error {
		beats++
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	now = now.Add(59 * time.Second)
	tr.Touch("qp1")
	now = now.Add(59 * time.Second)
	if err := tr.BeatIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if beats != 0 {
		t.Fatalf("beats = %d, want 0", beats)
	}
	now = now.Add(time.Second)
	if err := tr.BeatIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if beats != 1 {
		t.Fatalf("beats = %d, want 1", beats)
	}
}

func TestTrackerRecordsHeartbeatError(t *testing.T) {
	now := time.Unix(100, 0)
	tr, err := New(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tr.SetNow(func() time.Time { return now })
	want := errors.New("post send")
	if err := tr.Add("qp1", SenderFunc(func(context.Context) error {
		return want
	})); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if err := tr.BeatIdle(context.Background()); !errors.Is(err, want) {
		t.Fatalf("BeatIdle = %v, want %v", err, want)
	}
	st, ok := tr.Status("qp1")
	if !ok {
		t.Fatal("missing status")
	}
	if st.Healthy || st.LastError != want.Error() {
		t.Fatalf("status = %+v, want unhealthy error", st)
	}
	if st.Beats != 1 || st.Errors != 1 {
		t.Fatalf("status counters = %+v, want one beat one error", st)
	}
}

func TestTrackerDoesNotRetryUnhealthyRoute(t *testing.T) {
	now := time.Unix(100, 0)
	tr, err := New(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tr.SetNow(func() time.Time { return now })
	var calls int
	want := errors.New("post write")
	if err := tr.Add("qp1", SenderFunc(func(context.Context) error {
		calls++
		return want
	})); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if err := tr.BeatIdle(context.Background()); !errors.Is(err, want) {
		t.Fatalf("BeatIdle = %v, want %v", err, want)
	}
	now = now.Add(time.Minute)
	if err := tr.BeatIdle(context.Background()); err != nil {
		t.Fatalf("BeatIdle unhealthy = %v, want nil", err)
	}
	if calls != 1 {
		t.Fatalf("heartbeat calls = %d, want 1", calls)
	}
}

func TestTrackerRejectsBadInput(t *testing.T) {
	if _, err := New(0); err == nil {
		t.Fatal("New(0) = nil")
	}
	tr, err := New(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Add("", SenderFunc(func(context.Context) error { return nil })); err == nil {
		t.Fatal("Add empty = nil")
	}
	if err := tr.Add("qp1", nil); err == nil {
		t.Fatal("Add nil sender = nil")
	}
}
