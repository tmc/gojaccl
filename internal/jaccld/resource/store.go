package resource

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Store issues session leases from bounded daemon-owned resource pools.
// It manages logical leases only; daemon startup owns physical hardware
// initialization and supplies pre-created pools.
type Store struct {
	mu     sync.Mutex
	state  State
	now    func() time.Time
	next   LeaseID
	mr     MRPool
	qp     QueuePairPool
	cq     CompletionQueuePool
	leases map[LeaseID]SessionLease
}

// NewStore returns a Store backed by the given pools.
func NewStore(mr MRPool, qp QueuePairPool, cq CompletionQueuePool) (*Store, error) {
	if mr == nil {
		return nil, fmt.Errorf("new resource store: nil memory-region pool")
	}
	if qp == nil {
		return nil, fmt.Errorf("new resource store: nil queue-pair pool")
	}
	if cq == nil {
		return nil, fmt.Errorf("new resource store: nil completion-queue pool")
	}
	return &Store{
		state:  StateBootstrapping,
		now:    time.Now,
		mr:     mr,
		qp:     qp,
		cq:     cq,
		leases: make(map[LeaseID]SessionLease),
	}, nil
}

// State reports the daemon state used for lease admission.
func (s *Store) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// SetState records a forward daemon state transition.
func (s *Store) SetState(state State) error {
	if !state.valid() {
		return fmt.Errorf("%w: %s", ErrInvalidState, state)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == StateTerminated && state != StateTerminated {
		return fmt.Errorf("%w: %s after %s", ErrInvalidState, state, s.state)
	}
	if state < s.state {
		return fmt.Errorf("%w: %s after %s", ErrInvalidState, state, s.state)
	}
	s.state = state
	return nil
}

// Open leases daemon-owned resources for one client session.
func (s *Store) Open(ctx context.Context, req SessionRequest) (SessionLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	now := s.now()
	if err := validateRequest(req, now); err != nil {
		return SessionLease{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateReady {
		return SessionLease{}, ErrNotReady
	}
	window, err := s.mr.AllocMR(ctx, req.Size)
	if err != nil {
		return SessionLease{}, fmt.Errorf("open session: allocate memory window: %w", err)
	}
	cq, err := s.cq.AcquireCompletionQueue(ctx)
	if err != nil {
		cleanup := s.mr.FreeMR(ctx, window)
		return SessionLease{}, errors.Join(fmt.Errorf("open session: acquire completion queue: %w", err), cleanup)
	}
	qp, err := s.qp.AcquireQueuePair(ctx, req.Peer)
	if err != nil {
		cleanup := errors.Join(
			s.cq.ReleaseCompletionQueue(ctx, cq),
			s.mr.FreeMR(ctx, window),
		)
		return SessionLease{}, errors.Join(fmt.Errorf("open session: acquire queue pair: %w", err), cleanup)
	}
	s.next++
	lease := SessionLease{
		ID:              s.next,
		ClientID:        req.ClientID,
		Peer:            req.Peer,
		Window:          window,
		HeartbeatMR:     req.HeartbeatMR,
		QueuePair:       qp,
		CompletionQueue: cq,
		CreatedAt:       now,
		ExpiresAt:       req.Deadline,
		LastActivity:    now,
		Healthy:         true,
	}
	s.leases[lease.ID] = lease
	return lease, nil
}

// Refresh extends a session lease deadline.
func (s *Store) Refresh(id LeaseID, deadline time.Time) (SessionLease, error) {
	now := s.now()
	if id == 0 {
		return SessionLease{}, fmt.Errorf("%w: lease id is zero", ErrInvalidRequest)
	}
	if deadline.IsZero() {
		return SessionLease{}, fmt.Errorf("%w: deadline is zero", ErrInvalidRequest)
	}
	if !deadline.After(now) {
		return SessionLease{}, ErrExpired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateReady {
		return SessionLease{}, ErrNotReady
	}
	lease, ok := s.leases[id]
	if !ok {
		return SessionLease{}, ErrLeaseNotFound
	}
	if !lease.ExpiresAt.After(now) {
		return SessionLease{}, ErrExpired
	}
	lease.ExpiresAt = deadline
	lease.LastActivity = now
	lease.Healthy = true
	s.leases[id] = lease
	return lease, nil
}

// Touch records successful control-plane or data-plane activity for a lease.
func (s *Store) Touch(id LeaseID) (SessionLease, error) {
	now := s.now()
	if id == 0 {
		return SessionLease{}, fmt.Errorf("%w: lease id is zero", ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateReady {
		return SessionLease{}, ErrNotReady
	}
	lease, ok := s.leases[id]
	if !ok {
		return SessionLease{}, ErrLeaseNotFound
	}
	if !lease.ExpiresAt.After(now) {
		return SessionLease{}, ErrExpired
	}
	lease.LastActivity = now
	lease.Healthy = true
	s.leases[id] = lease
	return lease, nil
}

// PulseControlPlane records provider-free liveness for all live leases.
//
// It does not extend deadlines and does not touch provider, queue-pair, or
// completion-queue state.
func (s *Store) PulseControlPlane() int {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for id, lease := range s.leases {
		if !lease.ExpiresAt.After(now) {
			continue
		}
		lease.LastActivity = now
		lease.Healthy = true
		s.leases[id] = lease
		n++
	}
	return n
}

// RunControlPlaneLiveness periodically records provider-free lease liveness.
func (s *Store) RunControlPlaneLiveness(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("%w: liveness interval %s must be positive", ErrInvalidRequest, interval)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.PulseControlPlane()
		}
	}
}

// Lookup reports a session lease by ID.
func (s *Store) Lookup(id LeaseID) (SessionLease, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.leases[id]
	return lease, ok
}

// LookupLive reports a non-expired session lease by ID.
func (s *Store) LookupLive(id LeaseID) (SessionLease, error) {
	now := s.now()
	if id == 0 {
		return SessionLease{}, fmt.Errorf("%w: lease id is zero", ErrInvalidRequest)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateReady {
		return SessionLease{}, ErrNotReady
	}
	lease, ok := s.leases[id]
	if !ok {
		return SessionLease{}, ErrLeaseNotFound
	}
	if !lease.ExpiresAt.After(now) {
		return SessionLease{}, ErrExpired
	}
	return lease, nil
}

// Close releases one session lease.
func (s *Store) Close(ctx context.Context, id LeaseID) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.leases[id]
	if !ok {
		return ErrLeaseNotFound
	}
	delete(s.leases, id)
	return s.releaseLocked(ctx, lease)
}

// ReapExpired releases leases whose deadlines have passed.
func (s *Store) ReapExpired(ctx context.Context) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	var n int
	for id, lease := range s.leases {
		if lease.ExpiresAt.After(now) {
			continue
		}
		delete(s.leases, id)
		n++
		err = errors.Join(err, s.releaseLocked(ctx, lease))
	}
	return n, err
}

// Stats reports store and pool use.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		State:            s.state,
		Leases:           len(s.leases),
		MemoryRegions:    s.mr.MRStats(),
		QueuePairs:       s.qp.QueuePairStats(),
		CompletionQueues: s.cq.CompletionQueueStats(),
	}
}

func (s *Store) releaseLocked(ctx context.Context, lease SessionLease) error {
	return errors.Join(
		s.qp.ReleaseQueuePair(ctx, lease.QueuePair),
		s.cq.ReleaseCompletionQueue(ctx, lease.CompletionQueue),
		s.mr.FreeMR(ctx, lease.Window),
	)
}

func validateRequest(req SessionRequest, now time.Time) error {
	if strings.TrimSpace(req.ClientID) == "" {
		return fmt.Errorf("%w: client id is empty", ErrInvalidRequest)
	}
	if req.Peer.Rank < 0 {
		return fmt.Errorf("%w: peer rank %d is negative", ErrInvalidRequest, req.Peer.Rank)
	}
	if req.Size <= 0 {
		return fmt.Errorf("%w: size %d must be positive", ErrInvalidRequest, req.Size)
	}
	if req.Deadline.IsZero() {
		return fmt.Errorf("%w: deadline is zero", ErrInvalidRequest)
	}
	if !req.Deadline.After(now) {
		return ErrExpired
	}
	if req.Heartbeat < 0 {
		return fmt.Errorf("%w: heartbeat %s is negative", ErrInvalidRequest, req.Heartbeat)
	}
	if !req.HeartbeatMR.IsZero() {
		if err := req.HeartbeatMR.ValidateForRDMA(); err != nil {
			return err
		}
	}
	return nil
}
