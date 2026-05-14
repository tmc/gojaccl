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

## Deferred Work

Actual RDMA-write keepalive execution remains a later two-host gated slice. It
needs a remote heartbeat MR lease negotiated between daemon peers and a poll
path that can account for heartbeat completions without consuming user
completions incorrectly.

Do not replace this with SEND-based heartbeats on the data queue pair, and do
not treat an Apple-provider zero rkey as a valid heartbeat destination.
