# Go JACCL API Spec

This document records the Go API design for JACCL. It is based on the current
MLX JACCL C++ sources in this directory and Go API design principles.

## Source Facts

The C++ library exposes a `jaccl::Group` with `rank`, `size`, `all_sum`,
`all_max`, `all_min`, `all_gather`, `send`, `recv`, and `barrier` methods.
Reductions take raw input and output pointers, a byte count, and a `Dtype`
selector. The supported type IDs are bool, signed and unsigned integers,
float16, bfloat16, float32, float64, and complex64.

Configuration can come from environment variables or an explicit `Config`.
The direct backend uses `JACCL_RANK` or `MLX_RANK`, `JACCL_IBV_DEVICES` or
`MLX_IBV_DEVICES`, `JACCL_COORDINATOR` or `MLX_JACCL_COORDINATOR`, and optional
`JACCL_RING` or `MLX_JACCL_RING`. The daemon backend uses `JACCL_RANK`,
`JACCL_SIZE`, `JACCL_BACKEND=daemon`, and `JACCL_DAEMON_SOCKET`; `MLX_RANK`,
`MLX_WORLD_SIZE`, and `MLX_SIZE` are accepted fallbacks for rank and size. The
device file is a rank-by-rank JSON connectivity matrix.

The implementation chooses a ring group when ring is preferred and valid,
otherwise a mesh group when mesh is valid, otherwise a ring group if only ring
is valid. Mesh expects one usable device path for each non-self peer. Ring
expects symmetric adjacent connections.

Each group uses a TCP side channel to exchange RDMA destination data, then
brings queue pairs through initialization into a ready-to-send state. The RDMA
layer dynamically opens `librdma.dylib`, resolves `ibv_*` symbols, allocates
protection domains and completion queues, creates UC queue pairs, registers
page-aligned shared buffers, posts send and receive work requests, and polls
completion queues.

The current MLX adapter wraps the standalone `jaccl::Group` as an MLX
distributed group and dispatches blocking JACCL calls through the CPU command
encoder. `sum_scatter` and group splitting are explicitly unsupported.

## Design Goals

The Go API should be smaller than the C++ surface, not a transliteration of it.
The exported package should describe collectives, ranks, configuration, and
errors. It should not expose queue pairs, protection domains, memory regions,
TCP side-channel messages, or device handles.

The API should be synchronous and context-aware. RDMA operations are blocking
today; Go callers need cancellation, deadlines, and predictable cleanup when a
rank fails or a coordinator cannot be reached.

The API should separate typed collective operations from byte-oriented
point-to-point operations. Reductions and gather should preserve the caller's
element type; send and receive can stay byte-oriented because they move opaque
payloads between ranks.

The public package should not require cgo. Any Apple RDMA binding, dynamic
library loading, or generated ABI surface belongs behind build tags and internal
packages. The current RDMA backend is `darwin && arm64`; other platforms build
with stubs that return `rdma unavailable` from the internal backend.

## Package Shape

The package name should be `jaccl`.

Public API:

- `Config` describes rank, group size, backend selection, daemon socket,
  coordinator, device matrix, and whether ring should be preferred when the
  matrix is valid for direct ring communication.
- `Group` owns a live communicator and its internal RDMA resources.
- `NewGroup` initializes the communicator from an explicit `Config`.
- `NewGroupFromEnv` parses the direct backend environment contract inherited
  from MLX JACCL, plus Go-specific backend and daemon socket variables.
- `Available` reports whether the platform backend can load and use RDMA.
- `Rank() int` and `Size() int` report the local rank and group size.
- `Close() error` releases registered memory, completion queues, queue pairs,
  side-channel sockets, and OS resources.
- Typed package-level functions perform collectives on a `*Group`.
- `Barrier`, `Send`, and `Recv` are methods because they do not need a generic
  element type.

Internal packages:

- `internal/rdma` loads and wraps the Apple RDMA verbs surface.
- `internal/tcpchan` implements the rank side channel.
- `internal/topology` validates mesh and ring connectivity.
- `internal/reduce` contains local reduction kernels and dtype dispatch.

## Public API

The public package builds on all supported Go platforms. The RDMA backend is
available only on `darwin && arm64`; unsupported platforms report that RDMA is
unavailable.

Package declaration: `package jaccl`.

Imports: `context`.

Public configuration type:

- `type Config struct`
- `Rank int`
- `Size int`
- `Coordinator string`
- `Devices [][][]string`
- `PreferRing bool`
- `Backend string`
- `DaemonSocket string`

Public package functions:

- `func ConfigFromEnv() (Config, error)`
- `func Available() bool`
- `func NewGroup(ctx context.Context, cfg Config) (*Group, error)`
- `func NewGroupFromEnv(ctx context.Context) (*Group, error)`

Public group type and methods:

- `type Group struct` with unexported fields.
- `func (g *Group) Rank() int`
- `func (g *Group) Size() int`
- `func (g *Group) Barrier(ctx context.Context) error`
- `func (g *Group) Send(ctx context.Context, dst int, src []byte) error`
- `func (g *Group) Recv(ctx context.Context, src int, dst []byte) error`
- `func (g *Group) NewSendWriter(ctx context.Context, dst int) (*SendWriter, error)`
- `func (g *Group) NewRecvReader(ctx context.Context, src int) (*RecvReader, error)`
- `func (g *Group) Close() error`

Public type constraints:

- `type Element interface`: bool, signed integers, unsigned integers,
  `float32`, `float64`, and `complex64`.
- Exact type set: `bool | int8 | int16 | int32 | int64 | uint8 |
  uint16 | uint32 | uint64 | float32 | float64 | complex64`.

Public collective functions:

- `func AllSum[T Element](ctx context.Context, g *Group, dst, src []T) error`
- `func AllMax[T Element](ctx context.Context, g *Group, dst, src []T) error`
- `func AllMin[T Element](ctx context.Context, g *Group, dst, src []T) error`
- `func AllGather[T Element](ctx context.Context, g *Group, dst, src []T) error`

Float16 and bfloat16 should not be faked as ordinary `uint16` reductions in
the primary API. The first Go version therefore accepts only the predeclared
element types above, not user-defined aliases with the same underlying type.
If a later version needs half types before Go has standard types, add explicit
wrapper types and include them in the supported operation sets only after the
reduction semantics and conversion behavior are tested against the C++
implementation.

## API Rules

`NewGroup` validates the config, starts or connects to the side channel,
opens RDMA devices, creates the necessary queues and buffers, exchanges
destination information, and returns only after the group is ready for
operations. The passed context applies to the whole initialization.

The RDMA backend owns persistent registered staging buffers. It must not
register arbitrary caller Go heap slices, and it must not allocate and destroy
memory regions in the hot path. Operations copy caller data into mmap-backed
registered memory before posting RDMA work requests and copy received data back
out after completion.

Every blocking operation takes `context.Context`: `NewGroup`, `NewGroupFromEnv`,
`Barrier`, `Send`, `Recv`, `AllSum`, `AllMax`, `AllMin`, and `AllGather`.

All operations return errors. Expected failures must not panic. Errors should
carry the operation and rank context, for example `jaccl: rank 1 all sum:
poll completion queue: context deadline exceeded`.

`Close` is idempotent. It should release registered memory, queue pairs,
completion queues, protection domains, device contexts, side-channel sockets,
and any dynamically loaded backend handles owned by the group.

The caller owns input and output slices. The implementation may copy into
registered staging buffers internally. Public documentation must say whether
`dst` and `src` may alias. The C++ benchmarks use in-place all-sum, so in-place
operation should be supported unless a backend limitation forbids it.

Operations must reject length mismatches before touching RDMA state. For
`AllGather[T]`, `len(dst)` must be `g.Size()*len(src)`.

`AllGather` must not accept arbitrary Go values. Its type parameter is
constrained to `Element` so pointer-bearing values such as strings, maps,
slices, interfaces, and pointers cannot be copied across ranks as meaningless
process-local addresses.

The API should not export a `Dtype` enum unless an untyped escape hatch is
unavoidable. Generic functions give ordinary Go callers compile-time type
checking and avoid pointer-plus-byte-count mistakes.

The `Element` constraint is a wire-safety and dtype-dispatch constraint, not a
promise that Go operators implement every reduction. `AllMax` and `AllMin` must
match the C++ JACCL backend's dtype semantics, including its bool and complex64
handling, or explicitly reject a dtype before posting work.

Collective calls on a group must occur in the same order on every rank. A
single `Group` permits at most one active collective, point-to-point operation,
or stream at a time; implementations must serialize local access or return a
clear busy-state error before touching shared staging buffers. Stream adapters
hold the group operation from construction until `Close` or received EOF so
stream frames cannot interleave with another operation.

Zero-length collectives are local no-payload operations, but they still enter
the group operation path. A closed group or canceled context must be reported
even when the source slice is empty.

## Testing And Validation

Unit tests should cover config parsing, mesh and ring validation, environment
fallbacks, rank bounds, operation length checks, close idempotence, and dtype
selection.

Backend tests should skip when `Available` is false. They should never require
RDMA hardware for ordinary `go test ./...`.

The first integration gate should be a two-rank barrier test that mirrors
`minimal_barrier.cpp`: stagger rank entry, call `Barrier`, then run an `AllSum`
over `rank+1` and check the result is `size*(size+1)/2`.

On macOS, tests that transition queue pairs to RTR are manual hardware gates,
not normal developer tests. `JACCL_TEST_RDMA=1` may enable discovery and safe
failure-path checks, but any test that drives real queue-pair readiness must
also require a second explicit one-shot confirmation such as
`JACCL_TEST_RDMA_ALLOW_RTR=1`. Apple Thunderbolt RDMA is a physical
point-to-point fabric, so same-host local loopback RTR is not a valid default
integration topology. Direct `TestIntegrationChild` runs are part of the same
manual gate; they are not a bypass around the operator confirmation.

The benchmark gate should mirror `allreduce_bench.cpp` for float32 first, then
float16 and bfloat16 once the Go type story is settled. It should report
latency and bandwidth and include a correctness mode.

The scheduler gate is important. The current purego RDMA binding exposes
`Ibv_poll_cq` but no completion-event API. Completion polling must therefore
use a hybrid backoff loop: spin briefly, then yield with `runtime.Gosched` or a
short sleep, and check `ctx.Err()` regularly. A tight busy-poll loop that can
strand Go programs should not be accepted.

## Open Questions

The current C++ source uses UC queue pairs. The Go spec should not promise a
reliable-send semantic stronger than the backend provides.

The backend boundary should wrap the existing purego-based
`github.com/tmc/apple/rdma` bindings. Do not hand-edit generated RDMA binding
files; fix generator inputs and regenerate when the binding surface is wrong.

Float16 and bfloat16 need a compatibility decision. The C++ code has custom
fallback types and runtime bfloat16 feature detection; Go needs explicit types
and tests before those can be treated as first-class reductions.

## Implemented Surface

The Go module includes the public `jaccl` package, internal topology/reduce/TCP
side-channel packages, and a `darwin && arm64` purego RDMA wrapper over
`github.com/tmc/apple/rdma`.

The backend initializes RDMA devices, completion queues, queue pairs, persistent
staging memory, and the TCP side channel. Mesh collectives exchange directly
with connected peers. Ring collectives rotate payloads over adjacent links and
reduce locally. Hardware integration tests are present but require explicit
operator opt-in before driving the macOS RTR transition.

`cmd/jaccld` owns the daemon path. It opens one RDMA device, allocates one
protection domain, registers one shared slab memory region, exchanges daemon
queue-pair destinations over the TCP side channel, and serves local IPC clients
over a Unix-domain socket. The daemon backend is optional and explicit. It
supports barrier, send, recv, and daemon-supported collectives through slab
leases. Collective IPC uses a deliberate asynchronous work model: clients submit
work and then wait on a work ID.

Daemon-backed RDMA_WRITE heartbeats are disabled by default and are not the
Apple Thunderbolt RDMA production path. The experimental hook is opt-in and
must fail closed unless the peer publishes a real nonzero heartbeat address,
rkey, length, and epoch from a live heartbeat lease. Observed Apple Thunderbolt
RDMA registrations publish rkey zero, so RDMA_WRITE keepalive is rejected for
this provider's production envelope.

Control-plane liveness remains a daemon and lease health signal, but it does
not prove idle data-QP safety. Background same-data-QP SEND/RECV heartbeats are
rejected: receive matching is remote FIFO, and WR IDs are local completion
metadata, not wire tags. A remote user SEND can consume a local heartbeat RECV,
and a remote heartbeat SEND can consume a local user RECV. Completion demux
remains useful for normal traffic correctness after a completion is produced,
but it cannot make receive-queue matching safe.

The accepted data-QP path is explicit same-QP maintenance. It runs only after
all ranks stop admitting user operations, drain active daemon operations, hold
the relevant endpoint locks, and synchronize over TCP side-channel barriers
before and after reserved same-QP maintenance traffic. The physically proved
production envelope is two hosts and `rdma_en1`, with 45 successful maintenance
rounds over a long idle window and passing pre/post daemon-backed barrier-sum.
The control plane is proved with SSH-forwarded loopback `tcpchan` and, for the
documented `rdma_en1` IP pair only, direct non-loopback `tcpchan` with explicit
`-allow-remote-tcpchan`. A dedicated heartbeat QP may prove
daemon/provider/control-plane liveness, but it does not prove the user data QP
stayed warm.
