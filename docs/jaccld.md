# jaccld daemon design

`jaccld` owns the Apple Thunderbolt RDMA hardware lifecycle for one host.
The public Go JACCL API can use this daemon instead of allocating verbs
resources in each process.

Use of the daemon backend is explicit. `Config.Backend` accepts `auto`,
`direct`, and `daemon`; empty means `auto`. `auto` uses the working direct
backend today. `daemon` uses the jaccld IPC client for barrier and
point-to-point operations; daemon-backed collectives remain explicitly
unsupported until the IPC transport can support concurrent work.

## Constraints

Apple's Thunderbolt RDMA provider has small and fragile process-visible
resource limits:

- Protection domains are effectively leaked by the provider after teardown.
  The daemon must allocate one protection domain for its process lifetime and
  must not allocate a protection domain per client.
- Memory regions are limited enough that registering one region per tensor or
  transfer is not viable. The daemon must register one large slab and sublease
  byte ranges from it.
- Queue pairs become unreliable after roughly 23 idle minutes. The daemon must
  keep every active queue pair warm independently of user traffic.

These constraints rule out a library-only architecture. Short-lived Python,
MLX, or Go processes must not own RDMA PD or MR lifetimes directly.

## Process Model

On startup, `jaccld`:

1. Validates `-rank`, `-size`, and `-coordinator` unless `-no-rdma` is set.
2. Opens the configured RDMA device.
3. Allocates one protection domain.
4. Creates and registers one shared memory slab.
5. Creates queue pairs for the other daemon ranks and exchanges destinations
   on the TCP side channel.
6. Starts RDMA-write heartbeat management for active queue pairs.
7. Listens on a Unix-domain socket, by default `/tmp/jaccld.sock`.

The daemon releases RDMA resources only during daemon shutdown. Client
disconnect, crash, or cancellation may release logical leases, but must not
close the hardware context.

## Memory Model

The daemon backs the slab with shared memory so local clients can map the same
physical pages. The control plane passes the slab file descriptor over the UDS
using `SCM_RIGHTS`.

The backing file is unlinked after mmap succeeds. Clients receive the live file
descriptor, not a path, so a daemon crash must not strand a large temporary
file.

A client requests a range lease:

```text
alloc(size) -> lease ID, offset, length
map()       -> shared-memory file descriptor
free(id)    -> releases the logical range
```

The allocator coalesces freed ranges and rejects allocations outside the fixed
slab. The registered MR covers the entire slab, so transfers refer to offsets
within the one registered memory region.

## Keepalive Model

Apple's Thunderbolt RDMA provider requires active traffic to prevent queue-pair
degradation after roughly 23 minutes. The daemon reserves one byte in its
registered slab and publishes that byte's registered address and rkey with the
queue-pair destination metadata.

Each active peer route has a last-activity timestamp. When a route is idle for
the configured interval, the daemon posts a one-byte RDMA write to the peer's
reserved sink byte and waits for the local completion. This keeps the data queue
pair active without consuming peer receive work requests and without writing
into user payload buffers.

## IPC Model

The UDS carries small JSON control messages and file descriptors. The initial
protocol is intentionally small:

- `alloc`: allocate a range in the slab.
- `free`: release a range lease.
- `map`: return the slab descriptor with `SCM_RIGHTS`.
- `stats`: return slab usage and daemon health.
- `send`: ask the daemon transport to send a leased slab range to a peer.
- `recv`: ask the daemon transport to receive a peer transfer into a leased
  slab range.
- `barrier`: ask the daemon transport to synchronize active peers.

This is a control protocol, not a tensor planner. Tensor-parallel decisions and
mesh placement remain outside `jaccld`.

The initial IPC protocol is deliberately synchronous: one JSON request receives
one JSON response on one Unix-domain socket connection. Slab leases are scoped
to the connection that allocated them so the server can release memory when a
client crashes. Do not move collectives above the backend until the IPC layer
has an explicit asynchronous work protocol or equivalent design.

## Planner Data

`jaccld` publishes route observations for the higher-level mesh planner:

- peer identity and signed epoch,
- lease expiry,
- RTT,
- jitter,
- stale-measurement penalty,
- route health.

The daemon may evict a peer from its active routing table when the peer misses
lease expiry. It does not decide tensor parallelism policy.

## File Layout

- `cmd/jaccld/main.go`: command entry point, flags, signals, singleton hardware
  startup, and UDS listener.
- `cmd/jaccld/transport.go`: daemon-owned RDMA point-to-point transport over
  the registered slab.
- `internal/allocator/slab.go`: shared-memory slab allocator and logical leases.
- `internal/ipc/server.go`: UDS control server and `SCM_RIGHTS` descriptor
  passing.
- `internal/keepalive/heartbeat.go`: idle-route heartbeat scheduling.

## Stop Conditions

Do not bind `ibv_alloc_pd` to a UDS connection, a `Group`, or a client process.
Do not register memory for every tensor or transfer.
Do not use SEND-based heartbeats on the data queue pair; they can consume user
receives. Heartbeats must use RDMA write or an explicitly framed protocol.
