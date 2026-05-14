package main

import (
	"context"
	"fmt"
	"sync"
)

type admissionGate struct {
	mu          sync.Mutex
	maintenance bool
	inFlight    int
	changed     chan struct{}
}

func newAdmissionGate() *admissionGate {
	return &admissionGate{changed: make(chan struct{})}
}

// enter admits one data operation unless a maintenance window is active.
func (g *admissionGate) enter(ctx context.Context) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		g.mu.Lock()
		if !g.maintenance {
			g.inFlight++
			g.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(g.leave)
			}, nil
		}
		changed := g.changed
		g.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// beginMaintenance stops new data operations and waits for active ones to drain.
func (g *admissionGate) beginMaintenance(ctx context.Context) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g.mu.Lock()
	if g.maintenance {
		g.mu.Unlock()
		return nil, fmt.Errorf("maintenance already running")
	}
	g.maintenance = true
	g.signalLocked()
	for g.inFlight > 0 {
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			g.mu.Lock()
			g.maintenance = false
			g.signalLocked()
			g.mu.Unlock()
			return nil, ctx.Err()
		case <-changed:
		}
		g.mu.Lock()
	}
	g.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.maintenance = false
			g.signalLocked()
			g.mu.Unlock()
		})
	}, nil
}

func (g *admissionGate) leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inFlight == 0 {
		panic("jaccld admission gate: leave without enter")
	}
	g.inFlight--
	if g.inFlight == 0 {
		g.signalLocked()
	}
}

func (g *admissionGate) signalLocked() {
	close(g.changed)
	g.changed = make(chan struct{})
}
