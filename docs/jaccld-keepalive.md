# jaccld keepalive contract

`jaccld` must keep long-lived daemon routes observable without letting a
client request path allocate or probe RDMA resources. The production safety
signal is control-plane liveness. RDMA-write heartbeats remain disabled unless
the daemon has a real remote heartbeat memory lease.

## Default Signal

The resource store records `LastActivity` and `Healthy` on session leases.
Successful user traffic, session refresh, and control-plane liveness pulses may
update these fields. A liveness pulse must not extend `ExpiresAt`; expiry is
the admission contract, not a health timestamp.

`Store.PulseControlPlane` and `Store.RunControlPlaneLiveness` are provider-free
helpers. They do not import the RDMA provider, inspect queue pairs, poll
completion queues, or post work requests. They only update live lease metadata.

## RDMA Heartbeat Lease

An RDMA heartbeat may be armed only from an explicit heartbeat memory lease:

```go
type HeartbeatMR struct {
	Addr   uint64
	RKey   uint32
	Length int64
	Epoch  uint64
}
```

`Addr`, `RKey`, `Length`, and `Epoch` must all be nonzero, and `Length` must be
positive. A zero `Addr` or zero `RKey` fails closed before any future
RDMA-write heartbeat can be armed. A lease must also still be live at the time
the heartbeat metadata is requested.

The resource package records this contract with `SessionRequest.HeartbeatMR`,
`SessionLease.HeartbeatMR`, and `SessionLease.RDMAHeartbeatMR`. It does not
register memory, allocate protection domains, create queue pairs, or post
work requests.

Daemon peers also exchange heartbeat leases during startup. Each daemon
reserves a byte from the already-registered slab, publishes the real remote
address and rkey for that byte, and attaches a lease epoch and TTL. The
receiver rejects missing metadata, stale epochs, and expired leases before
arming an RDMA heartbeat.

## Deferred Work

The RDMA-write keepalive execution path remains gated behind
`-experimental-rdma-heartbeat` until the physical proof passes. When enabled it
posts a bounded one-byte RDMA write to the remote heartbeat MR lease, serializes
CQ polling with the connection lock, and records success/error counters in the
heartbeat tracker and daemon logs.

The remaining proof work is a two-host long-idle run that demonstrates those
heartbeats keep Apple Thunderbolt RDMA queue pairs usable beyond the known idle
failure window.

Do not replace this with SEND-based heartbeats on the data queue pair, and do
not treat an Apple-provider zero rkey as a valid heartbeat destination.
