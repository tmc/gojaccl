# jaccld data-QP keepalive proof plan

`jaccld` currently has a production-safe liveness model, not a proven
production data-QP keepalive. The accepted behavior is TCP/control-plane
liveness plus fail-closed datapath health. That keeps stale routes observable
without posting provider work from request paths, but it does not prove that an
idle Apple RDMA data QP remains warm.

This note records what would be required to remove that limitation.

## Current Stop Condition

Do not claim production data-QP keepalive until one of these paths has a
provider-safe design, implementation, review, and two-host long-idle proof:

- a remote-write primitive with a real nonzero remote key for a daemon-owned
  heartbeat byte;
- a two-sided operation that cannot consume or satisfy application receive FIFO
  entries;
- a globally quiescent maintenance collective that explicitly stops user
  traffic before posting same-QP SEND/RECV work.

Control-plane liveness, session lookup, and dedicated heartbeat QPs are useful
health signals, but they are not proof that the user data QP stayed warm.

## Rejected Background Paths

RDMA_WRITE heartbeats are not production on the observed Apple provider. The
daemon validates `HeartbeatMR` leases and fails closed on zero address, zero
remote key, zero length, zero epoch, stale leases, and expiry. Physical proof
so far shows Apple registered memory with remote key zero, so the validated
remote-write path cannot arm.

Asynchronous same-QP SEND/RECV heartbeats are rejected. RDMA receive matching
is FIFO on the remote receive queue. Work request IDs are local completion
metadata; they do not tag packets on the wire. A background heartbeat receive
can match a user send, and a background heartbeat send can match a user receive.

Dedicated heartbeat QPs are not a data-QP keepalive. They may prove daemon,
provider, side-channel, or CQ polling liveness, but they do not exercise the
application data QP that is known to go idle.

## Candidate Path: Maintenance Collective

A maintenance collective is the smallest remaining provider-correct direction
that does not require a nonzero remote key. It is not a background heartbeat.
It would be an explicit operation admitted by the daemon scheduler.

Minimum design requirements:

- stop admitting new user operations on all ranks in the group;
- wait for all in-flight daemon data operations to complete;
- hold the relevant connection locks on both endpoints;
- complete a TCP side-channel synchronization barrier after local admission is
  stopped and in-flight work is drained, so every peer knows all ranks are in
  maintenance state before any RDMA maintenance payload is posted;
- prove there are no outstanding application receives that could match the
  maintenance send;
- post reserved maintenance receives and sends on the target data QPs;
- poll with expected-completion matching;
- complete a second TCP side-channel synchronization barrier after RDMA
  maintenance completions and before any rank reopens user admission;
- poison the route on any timeout, provider error, unexpected completion, or
  mismatch;
- release locks and reopen admission only after every rank has completed the
  maintenance operation;
- never retry automatically after provider, RTR, CQ, or poison failure.

This design must reserve its own receive slots and buffers. It must not borrow
application receive FIFO capacity invisibly.

## No-Hardware Acceptance Gates

Before any hardware proof, the maintenance collective needs no-hardware tests
for:

- admission stops new user operations before maintenance begins;
- maintenance waits for in-flight operations to drain;
- the TCP side channel runs a barrier after local admission closes and before
  any rank posts maintenance RDMA work;
- the TCP side channel runs a barrier after maintenance completions and before
  any rank reopens user admission;
- locks are acquired in deterministic order and released on every error path;
- expected-completion matching handles unrelated completions without losing
  them;
- unexpected completion, timeout, and provider error poison the route;
- maintenance cannot run concurrently with send, receive, barrier, all-reduce,
  or all-gather paths;
- no request handler allocates PD, MR, QP, or CQ resources;
- `ALLOW_RTR` and integration gates do not drift.

Passing these tests would only make the design ready for review. It would not
prove the provider behavior.

## Physical Proof Gate

A successful production proof must use two physical Apple Thunderbolt RDMA
hosts with matching binaries and fresh preflight on both hosts. The run must:

- start `jaccld` on both hosts;
- run a datapath smoke test before idle;
- idle beyond the known failure window, at least 30 minutes and preferably
  45-60 minutes;
- run the maintenance operation on the data QPs during the idle period;
- capture daemon logs and counters proving the maintenance operation touched
  the target data QPs;
- run a datapath smoke test after idle;
- capture postflight provider state;
- stop at the first provider, RTR, CQ, poison, or timeout failure without
  automated retry.

Artifacts must include exact commands, binary hashes, stdout, stderr, exit
statuses, daemon logs, heartbeat or maintenance counters, preflight and
postflight state, and an explicit no-retry statement.

Until that artifact exists, the honest production statement remains:

`jaccld` provides provider-free control-plane liveness and fail-closed datapath
health. It does not yet provide a proven Apple RDMA data-QP keepalive.
