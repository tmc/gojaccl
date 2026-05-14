# jaccld daemon design

`jaccld` owns the Apple Thunderbolt RDMA hardware lifecycle for one host.
The public Go JACCL API can use this daemon instead of allocating verbs
resources in each process.

Use of the daemon backend is explicit. `Config.Backend` accepts `auto`,
`direct`, and `daemon`; empty means `auto`. `auto` uses the working direct
backend today. `daemon` uses the jaccld IPC client for barrier and
point-to-point operations. Daemon-backed collectives use asynchronous IPC work
when the daemon transport supports collective work.

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
  report route health conservatively; no accepted background data-QP heartbeat
  exists for Apple Thunderbolt RDMA yet.

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
6. Optionally starts experimental RDMA-write heartbeat management for active
   queue pairs.
7. Starts a bounded resource session store for local clients.
8. Starts provider-free control-plane liveness pulses unless disabled.
9. Listens on a Unix-domain socket, by default `/tmp/jaccld.sock`.

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
degradation after roughly 23 minutes, but daemon-backed RDMA heartbeats are not
enabled in the production path yet. The default daemon does not post heartbeat
work requests on the data queue pair. Successful user traffic still refreshes
route activity so a future heartbeat policy has a clear idle signal.

The experimental RDMA-write heartbeat path is opt-in only with
`-experimental-rdma-heartbeat`. It requires nonzero remote heartbeat address and
rkey metadata, a positive `-heartbeat-timeout`, and a positive
`-heartbeat-lease-ttl`; otherwise startup fails closed. Physical Apple provider
artifacts have shown registered memory with remote key zero, so RDMA-write is
not the production keepalive direction for this provider.

A production keepalive uses control-plane liveness as a health signal but not
as proof that the data QP stayed warm. Background same-QP SEND/RECV heartbeats
are rejected because receive matching is remote FIFO and WR IDs are local
completion metadata, not wire tags. A remote user SEND can consume a locally
posted heartbeat RECV, and a remote heartbeat SEND can consume a locally posted
user RECV. Completion demux is still useful after completions arrive, but it
does not make receive matching safe.

Heartbeat failures mark the route unhealthy without tearing down the
daemon-owned device, protection domain, or registered memory region. Any post,
poll, timeout, or provider error poisons the route and must not start a retry
loop. A dedicated heartbeat QP may prove daemon/provider/control-plane
liveness, but it is not evidence that the user data QP stayed warm. A globally
quiescent maintenance collective could safely run same-QP SEND/RECV traffic in
theory, but that is not a background keepalive and is outside this slice.

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
- `session_open`: lease a daemon-owned logical MR window and route handles.
- `session_refresh`: extend a session lease deadline.
- `session_lookup`: return current live session lease metadata, including
  provider-free liveness fields; expired leases fail closed.
- `session_close`: release a session lease.
- `session_stats`: return resource-store use.
- `submit_reduce`: start daemon-owned all-reduce work over leased slab ranges.
- `submit_gather`: start daemon-owned all-gather work over leased slab ranges.
- `wait_work`: poll asynchronous daemon work for completion.

This is a control protocol, not a tensor planner. Tensor-parallel decisions and
mesh placement remain outside `jaccld`.

Most IPC operations are request/response. Collective work is explicitly
asynchronous: a submit request returns a work ID, and `wait_work` reports
completion without blocking unrelated control requests. Slab leases are scoped
to the connection that allocated them so the server can release memory when a
client crashes, but disconnect waits for in-flight work to stop before freeing
those leases. Resource session leases are also scoped to the connection, so
disconnect releases the logical MR, queue-pair, and completion-queue handles.

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
- `cmd/jaccld/admission.go`: provider-free admission gate used to stop new
  daemon data operations and wait for in-flight work before a future
  maintenance window.
- `cmd/jaccld/transport.go`: daemon-owned RDMA point-to-point and collective
  transport over the registered slab, completion demux, heartbeat MR lease
  exchange, and the gated experimental RDMA-write heartbeat hook.
- `internal/allocator/slab.go`: shared-memory slab allocator and logical leases.
- `internal/ipc/server.go`: UDS control server and `SCM_RIGHTS` descriptor
  passing.
- `internal/jaccld/resource`: bounded resource session leases and provider-free
  pool interfaces.
- `docs/jaccld-keepalive.md`: provider-free keepalive contract and heartbeat
  MR lease rules.
- `docs/jaccld-data-qp-keepalive.md`: remaining data-QP keepalive stop
  condition, rejected paths, and proof gates.
- `internal/keepalive/heartbeat.go`: idle-route heartbeat scheduling.

## Stop Conditions

Do not bind `ibv_alloc_pd` to a UDS connection, a `Group`, or a client process.
Do not register memory for every tensor or transfer.
Do not use background SEND heartbeats on the data queue pair; they can consume
user receives. Do not enable RDMA-write heartbeats unless the remote heartbeat
memory window has a real nonzero address and rkey. Treat dedicated heartbeat
QPs as liveness-only unless a separate proof shows they preserve idle data-QP
health.
