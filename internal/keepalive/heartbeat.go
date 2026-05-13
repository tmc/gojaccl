// Package keepalive tracks idle routes that need daemon-owned heartbeats.
package keepalive

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Sender posts one heartbeat work request.
type Sender interface {
	Heartbeat(context.Context) error
}

// SenderFunc adapts a function into a Sender.
type SenderFunc func(context.Context) error

// Heartbeat calls f(ctx).
func (f SenderFunc) Heartbeat(ctx context.Context) error {
	return f(ctx)
}

// Status reports the latest route state tracked by Tracker.
type Status struct {
	LastActivity time.Time
	LastBeat     time.Time
	LastError    string
	Healthy      bool
}

type route struct {
	sender Sender
	status Status
}

// Tracker posts heartbeats for idle routes.
type Tracker struct {
	mu       sync.Mutex
	interval time.Duration
	now      func() time.Time
	routes   map[string]*route
}

// New returns a Tracker that sends heartbeats after interval of idleness.
func New(interval time.Duration) (*Tracker, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("new heartbeat tracker: interval %s must be positive", interval)
	}
	return &Tracker{
		interval: interval,
		now:      time.Now,
		routes:   make(map[string]*route),
	}, nil
}

// SetNow replaces the clock used by the tracker.
func (t *Tracker) SetNow(now func() time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.now = now
}

// Add starts tracking id.
func (t *Tracker) Add(id string, sender Sender) error {
	if id == "" {
		return fmt.Errorf("add heartbeat route: empty id")
	}
	if sender == nil {
		return fmt.Errorf("add heartbeat route %q: nil sender", id)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	t.routes[id] = &route{
		sender: sender,
		status: Status{
			LastActivity: now,
			Healthy:      true,
		},
	}
	return nil
}

// Remove stops tracking id.
func (t *Tracker) Remove(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.routes, id)
}

// Touch records user traffic on id.
func (t *Tracker) Touch(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r := t.routes[id]; r != nil {
		r.status.LastActivity = t.now()
	}
}

// Status reports the route status for id.
func (t *Tracker) Status(id string) (Status, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.routes[id]
	if r == nil {
		return Status{}, false
	}
	return r.status, true
}

// BeatIdle posts heartbeats for routes idle longer than the configured interval.
func (t *Tracker) BeatIdle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	type due struct {
		id     string
		sender Sender
	}
	now := t.snapshotTime()
	var work []due
	t.mu.Lock()
	for id, r := range t.routes {
		if now.Sub(r.status.LastActivity) >= t.interval {
			work = append(work, due{id: id, sender: r.sender})
		}
	}
	t.mu.Unlock()

	var first error
	for _, w := range work {
		err := w.sender.Heartbeat(ctx)
		t.recordBeat(w.id, err)
		if err != nil && first == nil {
			first = fmt.Errorf("heartbeat %s: %w", w.id, err)
		}
	}
	return first
}

// Run sends idle heartbeats until ctx is canceled.
func (t *Tracker) Run(ctx context.Context) error {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := t.BeatIdle(ctx); err != nil && ctx.Err() != nil {
				return err
			}
		}
	}
}

func (t *Tracker) snapshotTime() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.now()
}

func (t *Tracker) recordBeat(id string, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := t.routes[id]
	if r == nil {
		return
	}
	now := t.now()
	r.status.LastBeat = now
	if err != nil {
		r.status.LastError = err.Error()
		r.status.Healthy = false
		return
	}
	r.status.LastActivity = now
	r.status.LastError = ""
	r.status.Healthy = true
}
