# jaccld daemon design

`jaccld` owns the Apple Thunderbolt RDMA hardware lifecycle for one host.
The public Go JACCL API should become a client of this daemon rather than
allocating verbs resources in each process.

Use of the daemon backend is explicit. `Config.Backend` accepts `auto`,
`direct`, and `daemon`; empty means `auto`. Until the daemon data path is wired
into public collectives, `auto` uses the working direct backend and `daemon`
fails fast rather than silently falling back to direct RDMA ownership.

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

1. Opens the configured RDMA device.
2. Allocates one protection domain.
3. Creates and registers one shared memory slab.
4. Listens on a Unix-domain socket, by default `/tmp/jaccld.sock`.
5. Starts keepalive management for active queue pairs.

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

Each active queue pair has a last-activity timestamp. A heartbeat loop posts a
zero-byte or one-byte send after the queue pair is idle for the configured
interval, initially 60 seconds. Real sends and receives update the activity
timestamp.

Heartbeat failures mark the route unhealthy. The daemon reports unhealthy
routes to clients and planners instead of hiding transport degradation.

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
- `internal/allocator/slab.go`: shared-memory slab allocator and logical leases.
- `internal/ipc/server.go`: UDS control server and `SCM_RIGHTS` descriptor
  passing.
- `internal/keepalive/heartbeat.go`: idle queue-pair heartbeat scheduling.

## Stop Conditions

Do not bind `ibv_alloc_pd` to a UDS connection, a `Group`, or a client process.
Do not register memory for every tensor or transfer.
Do not rely on user traffic to keep queue pairs alive.
