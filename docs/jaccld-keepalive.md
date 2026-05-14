# jaccld keepalive contract

`jaccld` must keep long-lived daemon routes observable without letting a
client request path allocate or probe RDMA resources. The production safety
signal is control-plane liveness. Control-plane liveness proves daemon reach,
not idle data-QP health. Apple Thunderbolt RDMA data-QP warmth is handled by
an explicit same-data-QP maintenance operation in the supported proof envelope,
not by an asynchronous background heartbeat.

## Default Signal

The resource store records `LastActivity` and `Healthy` on session leases.
Successful user traffic, session refresh, and control-plane liveness pulses may
update these fields. A liveness pulse must not extend `ExpiresAt`; expiry is
the admission contract, not a health timestamp.

`Store.PulseControlPlane` and `Store.RunControlPlaneLiveness` are provider-free
helpers. They do not import the RDMA provider, inspect queue pairs, poll
completion queues, or post work requests. They only update live lease metadata.
`jaccld` runs this loop with `-control-plane-liveness`; the default command
interval is one minute, and zero disables the loop.

## RDMA-Write Lease

The RDMA-write experiment may be armed only from an explicit heartbeat memory
lease:

```go
type HeartbeatMR struct {
	Addr   uint64
	RKey   uint32
	Length int64
	Epoch  uint64
}
```

`Addr`, `RKey`, `Length`, and `Epoch` must all be nonzero, and `Length` must be
positive. A zero `Addr` or zero `RKey` fails closed before any RDMA-write
heartbeat can be armed. A lease must also still be live at the time the
heartbeat metadata is requested.

The resource package records this contract with `SessionRequest.HeartbeatMR`,
`SessionLease.HeartbeatMR`, and `SessionLease.RDMAHeartbeatMR`. It does not
register memory, allocate protection domains, create queue pairs, or post
work requests.

Daemon peers also exchange heartbeat leases during startup. Each daemon
reserves a byte from the already-registered slab, publishes the real remote
address and rkey for that byte, and attaches a lease epoch and TTL. The
receiver rejects missing metadata, stale epochs, and expired leases before
arming an RDMA heartbeat. This exchange only happens when
`-experimental-rdma-heartbeat` is enabled; the default daemon data path does
not require a nonzero remote memory key.

Apple Thunderbolt RDMA has published zero rkeys in physical proof artifacts,
even when the local registration requested remote access. That means
RDMA_WRITE keepalive is rejected for this provider's production path. The hook
can remain as an opt-in staging surface for future providers, but it is not a
production Apple Thunderbolt RDMA claim.

## Rejected Background Heartbeats

Asynchronous same-QP SEND/RECV heartbeats are rejected. RDMA receive matching is
remote FIFO; WR IDs are local completion metadata and do not tag messages on
the wire. A daemon cannot make a 1-byte heartbeat receive safe merely by taking
its local `conn.mu` and assigning a heartbeat WR ID.

The concrete failure mode is a cross-match between heartbeat and user traffic:
a remote user SEND can consume a locally posted heartbeat RECV, or a remote
heartbeat SEND can consume a locally posted user RECV. Either case corrupts or
poisons the data QP. Completion demux is still useful after a completion
arrives, but it cannot change receive-queue matching.

The accepted data-QP path is not asynchronous. It is a globally quiescent
maintenance operation: all ranks stop admitting user operations, drain
in-flight daemon operations, hold the relevant endpoint locks, run a TCP
side-channel pre-barrier, post reserved same-data-QP maintenance SEND/RECV
traffic, poll for expected completions, run a TCP side-channel post-barrier,
and only then reopen user traffic. Any barrier, provider, CQ, timeout,
unexpected completion, or poison failure is terminal for that route and must
not trigger an automated retry loop.

A dedicated heartbeat QP may be useful as daemon/provider/control-plane
liveness, but it does not prove the user data QP stayed warm.

## Production Envelope

The proven Apple Thunderbolt RDMA envelope is two physical hosts, RDMA pinned
to `rdma_en1`, daemon-owned slabs and queue pairs, explicit same-data-QP
maintenance during idle, and fail-closed route health. The control plane is
proved with SSH-forwarded loopback `tcpchan` and, for the documented `rdma_en1`
IP pair only, direct non-loopback `tcpchan` with explicit
`-allow-remote-tcpchan`.

The preserved proof artifacts are:

- `/Users/tmc/tmp/gojaccl-jaccld-dataqp-maintenance-proof-sshchan-20260514T090333Z`
- `/Users/tmc/tmp/gojaccl-direct-tcpchan-rdma-en1-proof-20260514T224843Z`

RDMA_WRITE heartbeat production readiness, arbitrary rank counts,
non-`rdma_en1` devices, and arbitrary non-loopback deployments remain excluded
until separate artifacts prove them.
