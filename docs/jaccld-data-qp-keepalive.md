# jaccld data-QP maintenance proof status

`jaccld` has a proven Apple Thunderbolt RDMA data-QP maintenance path for one
explicit deployment envelope. It is not a background heartbeat and it is not a
general topology claim.

## Accepted Claim

At commit `7a06b692cf2241c1c03702c3bab5ec252d4c308e`, explicit
same-data-QP maintenance passed a two-host Apple Thunderbolt RDMA proof with:

- RDMA pinned to `rdma_en1` on both hosts;
- `tcpchan` carried only over SSH loopback forwards;
- matching binaries on both hosts;
- fresh preflight and postflight provider state;
- pre-idle daemon-backed barrier-sum passing;
- 45/45 maintenance rounds returning `ok=true` across a 47-minute idle window;
- post-idle daemon-backed barrier-sum passing;
- postflight `rdma_en1` still active;
- no automated retry after provider, RTR, CQ, maintenance, barrier, poison, or
  postflight failure;
- clean daemon and tunnel cleanup.

The preserved artifact is:

```text
/Users/tmc/tmp/gojaccl-jaccld-dataqp-maintenance-proof-sshchan-20260514T090333Z
/Users/tmc/tmp/gojaccl-jaccld-dataqp-maintenance-proof-sshchan-20260514T090333Z.tar.gz
sha256 fd36e9726440a1224fafc9890184bbbc5321c114c3390baca25c2c7d2c054c67
```

The accepted production statement is:

`jaccld` provides daemon-owned RDMA resources, provider-free control-plane
lease health, fail-closed datapath health, and explicit same-data-QP
maintenance for the documented two-host `rdma_en1` plus SSH-forwarded
`tcpchan` deployment.

## Maintenance Operation

The maintenance operation is admitted explicitly through daemon control, for
example:

```sh
jacclctl -socket /tmp/jaccld.sock maintain
```

It must:

- stop admitting new user operations on all ranks in the group;
- wait for in-flight daemon data operations to complete;
- hold the relevant connection locks;
- complete a TCP side-channel pre-barrier after admission is closed and before
  any maintenance RDMA work is posted;
- prove there are no pending completions that could cross-match with
  maintenance work;
- post reserved maintenance receives and sends on the target data QPs;
- poll with expected-completion matching;
- complete a TCP side-channel post-barrier after RDMA maintenance completions
  and before any rank reopens admission;
- poison the route on any timeout, provider error, unexpected completion,
  barrier failure, or mismatch;
- never retry automatically after provider, RTR, CQ, poison, or postflight
  failure.

This operation reserves its own maintenance bytes from the daemon-owned
registered slab. It must not borrow application receive FIFO capacity
invisibly.

## Rejected Paths

RDMA_WRITE heartbeats are not production on the observed Apple provider. The
daemon validates `HeartbeatMR` leases and fails closed on zero address, zero
remote key, zero length, zero epoch, stale leases, and expiry. Physical proof
has shown Apple registered memory with remote key zero, so the remote-write
path cannot be the Apple production keepalive.

Asynchronous same-QP SEND/RECV heartbeats are rejected. RDMA receive matching
is FIFO on the remote receive queue. Work request IDs are local completion
metadata; they do not tag packets on the wire. A background heartbeat receive
can match a user send, and a background heartbeat send can match a user receive.

Dedicated heartbeat QPs are not a data-QP keepalive. They may prove daemon,
provider, side-channel, or CQ polling liveness, but they do not exercise the
application data QP that is known to go idle.

## Remaining Exclusions

The proof does not establish:

- direct Go TCP control-plane production readiness over non-loopback
  interfaces;
- RDMA_WRITE heartbeat production readiness;
- arbitrary `size > 2` topologies;
- non-`rdma_en1` Thunderbolt RDMA layouts;
- non-SSH-forwarded `tcpchan` deployments;
- automated cluster deployment readiness.

Direct non-loopback `tcpchan` use is intentionally fail-closed by default and
requires both `jacclctl tcp-diagnostic` proof and `-allow-remote-tcpchan`.
Broader topology support needs its own no-hardware review, bounded physical
proof, preserved artifacts, and explicit acceptance before this document can
claim it.
