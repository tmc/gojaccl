# Go JACCL Package File Layout

This document records the Go file layout for the current implementation.

The layout keeps the public package small, hides transport details in internal
packages, and avoids introducing new packages until the implementation needs
them.

## `gojaccl`

Package name: `jaccl`.

Implementation files:

- `doc.go`: package documentation.
- `config.go`: `Config`, backend-mode constants, `ConfigFromEnv`,
  device-matrix file loading, daemon socket configuration, and configuration
  validation.
- `group.go`: `Group`, `NewGroup`, `NewGroupFromEnv`, `Rank`, `Size`, `Close`,
  lifecycle state, and operation serialization.
- `collective.go`: `Element`, `AllSum`, `AllMax`, `AllMin`, `AllGather`, slice
  length checks, and alias checks.
- `p2p.go`: `Barrier`, `Send`, and `Recv`.
- `backend.go`: unexported backend interface, backend selection, and the bridge
  from `Group` methods to internal transport packages.
- `daemon_backend.go`: jaccld IPC client backend for barrier, point-to-point
  operations, and daemon-supported collectives.
- `errors.go`: error wrapping helpers and package sentinel errors.

Test and benchmark files:

- `backend_test.go`: backend selection, `Available`, and backend abstraction
  tests.
- `daemon_backend_test.go`: hardware-free daemon backend tests using an
  in-process IPC server and fake transport, including async collective work.
- `config_test.go`: environment parsing, backend selection, daemon socket, and
  configuration validation tests.
- `group_test.go`: group construction, lifecycle, close, and operation
  serialization tests.
- `collective_test.go`: collective length, cancellation, zero-length,
  in-place, element-dispatch, and type-constraint tests.
- `p2p_test.go`: `Barrier`, `Send`, and `Recv` tests.
- `errors_test.go`: error wrapping and error-context tests.
- `fake_test.go`: in-memory backend used by public package tests.
- `integration_test.go`: hardware integration tests. Safe discovery/failure
  checks require `JACCL_TEST_RDMA=1`; tests that transition queue pairs to RTR
  also require `JACCL_TEST_RDMA_ALLOW_RTR=1` and refuse same-host local
  loopback unless explicitly overridden.
- `benchmark_test.go`: benchmark entry points matching the C++ benchmark gate.
- `example_test.go`: runnable public examples.

## `gojaccl/internal/topology`

Package name: `topology`.

Files:

- `doc.go`: package documentation.
- `topology.go`: device-matrix validation, mesh validation, ring validation,
  and topology choice.
- `topology_test.go`: validation and choice tests.

Do not split mesh and ring validation into separate packages. They are policy
inside one topology decision.

## `gojaccl/internal/reduce`

Package name: `reduce`.

Files:

- `doc.go`: package documentation.
- `dtype.go`: internal dtype identifiers and mapping from public `jaccl`
  element types to backend dtype values.
- `reduce.go`: local sum, max, and min kernels.
- `reduce_test.go`: dtype mapping, local kernel, in-place, and bad-length
  tests.

Do not export a public dtype enum from `jaccl`. The dtype mapping is an
implementation detail.

## `gojaccl/internal/tcpchan`

Package name: `tcpchan`.

Files:

- `doc.go`: package documentation.
- `tcpchan.go`: side-channel type, coordinator listen/connect, metadata
  gather, barrier, and close behavior.
- `frame.go`: wire frame encoding and decoding for rank metadata and RDMA
  destination data.
- `tcpchan_test.go`: dial, metadata gather, barrier, and close tests.

Keep this package about initialization and control-plane metadata only. Data
movement remains in the public package and `internal/rdma`.

## `gojaccl/internal/rdma`

Package name: `rdma`.

Files:

- `doc.go`: package documentation.
- `rdma.go`: portable internal types and the package-level API used by
  `jaccl`.
- `rdma_unsupported.go`: non-`darwin/arm64` stubs returning unavailable
  errors.
- `rdma_darwin_arm64.go`: the Darwin/ARM64 purego verbs wrapper, including
  dynamic loading, device context ownership, protection domains, completion
  queues, UC queue pairs, mmap-backed staging memory registration, work
  request posting including RDMA write, completion polling, context checks, and
  hybrid backoff.
- `rdma_test.go`: loader, device, queue, memory, work request, polling, and
  purego-boundary tests.

This package must not use cgo. Incorrect generated binding details should be
fixed in the generator or binding source, not by hand-editing generated files.
If `rdma_darwin_arm64.go` becomes too large to navigate after implementation,
split it by measured subsystem size, not before.

## `gojaccl/internal/allocator`

Package name: `allocator`.

Files:

- `slab.go`: file-backed shared-memory slab, immediate unlink after mmap,
  logical byte-range leases, lease coalescing, FD access for descriptor
  passing, mmap lifecycle, and stats.
- `slab_test.go`: allocation, free/coalesce, error, shared mapping, and close
  tests.

Keep this package independent of RDMA verbs and IPC. It is lease math plus the
shared backing store.

## `gojaccl/internal/ipc`

Package name: `ipc`.

Files:

- `protocol.go`: small JSON control protocol shared by client and server,
  including slab lease, map, stats, barrier, send, recv, and resource session
  operations.
- `server.go`: Unix-domain socket server, `alloc`, `free`, `map`, `stats`, and
  `SCM_RIGHTS` file descriptor passing, resource session lifecycle dispatch,
  plus data-path dispatch through an injected `Transport`.
- `client.go`: local daemon client, slab mapping, data-path requests, and
  resource session requests.
- `ipc_test.go`: hardware-free UDS, FD-passing, mmap, disconnect cleanup,
  transport dispatch, resource session cleanup, lease-bound range validation,
  and missing-transport tests.

Keep this package as a local control plane. It must not decide tensor placement
or allocate RDMA hardware resources per connection. Data movement is expressed
only as peer plus slab-offset ranges; the injected transport owns how those
ranges reach RDMA.

## `gojaccl/internal/jaccld/resource`

Package name: `resource`.

Files:

- `doc.go`: package documentation and daemon resource invariants.
- `types.go`: daemon state, peer specs, memory windows, session requests,
  session leases, handles, stats, and errors.
- `pool.go`: provider-free MR, queue-pair, and completion-queue pool
  interfaces.
- `store.go`: session lease store, state transitions, open, refresh, close,
  provider-free liveness, expiry, cleanup, and stats.
- `slab.go`: allocator-backed MR window pool.
- `handle.go`: bounded static queue-pair and completion-queue handle pools for
  offline session accounting.
- `store_test.go`: lease lifecycle, state, validation, heartbeat MR fail-closed
  checks, exhaustion, liveness, refresh, and expiry tests.
- `pool_test.go`: slab-backed MR and static-handle pool tests.
- `static_test.go`: import and symbol guard preventing provider calls from
  entering the resource package.

Keep this package provider-free. Hardware-backed pools may implement these
interfaces later, but request handlers should only talk to the store.

## `gojaccl/internal/keepalive`

Package name: `keepalive`.

Files:

- `heartbeat.go`: idle-route tracker and heartbeat sender abstraction.
- `heartbeat_test.go`: idle, touch, error counter, and bad-input tests using
  fake senders and a fake clock.

This package schedules daemon-owned keepalives. The production daemon does not
post RDMA-write heartbeats by default. `jaccld` uses this idle signal to decide
when a provider-specific heartbeat policy should run, but Apple has no accepted
background data-QP heartbeat yet.

## `gojaccl/cmd/jaccld`

Package name: `main`.

Files:

- `main.go`: command flags, signal handling, shared slab creation, bounded
  resource session store creation, singleton RDMA device/protection-domain/MR
  startup, daemon rank validation, heartbeat lease TTL validation, transport
  injection, and IPC listener startup.
- `transport.go`: daemon-owned RDMA transport, side-channel destination
  exchange, queue-pair setup, slab-offset send, recv, collectives, completion
  demux, heartbeat MR lease exchange, gated experimental RDMA-write heartbeat
  setup, heartbeat poison-on-error behavior, barrier, and transport close
  behavior.
- `main_test.go`: hardware-free command validation and `-no-rdma` IPC smoke
  tests.
- `transport_test.go`: hardware-free daemon collective offset and reduction
  tests using slab-backed copy closures instead of RDMA provider calls.

The command may be run with `-no-rdma` for local IPC development, but production
startup must validate `-rank`, `-size`, and `-coordinator`, open the hardware,
register the single global slab, and connect peer daemon ranks before serving
clients.

Do not use background SEND heartbeats on the data queue pair. Receive matching
is remote FIFO, and WR IDs cannot prevent heartbeat/user cross-matches.
RDMA-write heartbeats remain experimental and fail closed on Apple-provider zero
rkeys. If any heartbeat post or poll fails, poison the connection and fail later
user traffic closed rather than risking late-CQE misattribution. Dedicated
heartbeat QPs may be useful for liveness, but not as proof that the data QP
stayed warm.

## Design Notes

- `docs/jaccld.md`: daemon process model, IPC protocol, and resource ownership
  constraints.
- `docs/jaccld-keepalive.md`: provider-free liveness contract and heartbeat MR
  lease requirements.

## Files Not To Add Yet

- Do not add `go.sum` directly.
- Do not add a `pkg/` tree.
- Do not add public `mesh`, `ring`, `dtype`, `rdma`, or `tcpchan` packages.
- Do not add `Float16` or `BFloat16` public files until parity tests against
  C++ JACCL semantics exist.
- Do not add examples directories until the package API is implemented and the
  tests can run real examples.
