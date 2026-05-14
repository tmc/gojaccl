# jaccld keepalive contract

`jaccld` must keep long-lived daemon routes observable without letting a
client request path allocate or probe RDMA resources. The production safety
signal is control-plane liveness. Control-plane liveness proves daemon reach,
not idle data-QP health. No background data-QP heartbeat is accepted for Apple
Thunderbolt RDMA yet.

## Default Signal

The resource store records `LastActivity` and `Healthy` on session leases.
Successful user traffic, session refresh, and control-plane liveness pulses may
update these fields. A liveness pulse must not extend `ExpiresAt`; expiry is
the admission contract, not a health timestamp.

`Store.PulseControlPlane` and `Store.RunControlPlaneLiveness` are provider-free
helpers. They do not import the RDMA provider, inspect queue pairs, poll
completion queues, or post work requests. They only update live lease metadata.

## RDMA-Write Lease

The earlier RDMA-write experiment may be armed only from an explicit heartbeat
memory lease:

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

Apple Thunderbolt RDMA has so far published zero rkeys in physical proof
artifacts, even when the local registration requested remote access. That means
RDMA-write keepalive is not the production direction for this provider.

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

A globally quiescent maintenance operation could make same-QP SEND/RECV traffic
safe in theory: all ranks would have to stop admitting user operations, hold
the relevant endpoint locks, prove all outstanding protocol sends and receives
completed, run the heartbeat as an explicit collective, and then reopen user
traffic. That is not a background keepalive and is outside the current slice.

A dedicated heartbeat QP may be useful as daemon/provider/control-plane
liveness, but it does not prove the user data QP stayed warm.

## Deferred Work

The production behavior remains TCP/control-plane liveness plus fail-closed
datapath health. The remaining open problem is a provider-safe way to keep the
actual data QP warm without consuming application receive FIFO entries.
