# Go JACCL Package File Layout

This document records the Go file layout for the current implementation.

The layout keeps the public package small, hides transport details in internal
packages, and avoids introducing new packages until the implementation needs
them.

## `gojaccl`

Package name: `jaccl`.

Implementation files:

- `doc.go`: package documentation.
- `config.go`: `Config`, `ConfigFromEnv`, device-matrix file loading, and
  configuration validation.
- `group.go`: `Group`, `NewGroup`, `NewGroupFromEnv`, `Rank`, `Size`, `Close`,
  lifecycle state, and operation serialization.
- `collective.go`: `Element`, `AllSum`, `AllMax`, `AllMin`, `AllGather`, slice
  length checks, and alias checks.
- `p2p.go`: `Barrier`, `Send`, and `Recv`.
- `backend.go`: unexported backend interface, backend selection, and the
  bridge from `Group` methods to internal transport packages.
- `errors.go`: error wrapping helpers and package-local sentinel errors.

Test and benchmark files:

- `backend_test.go`: backend selection, `Available`, and backend abstraction
  tests.
- `config_test.go`: environment parsing and configuration validation tests.
- `group_test.go`: group construction, lifecycle, close, and operation
  serialization tests.
- `collective_test.go`: collective length, cancellation, zero-length,
  in-place, element-dispatch, and type-constraint tests.
- `p2p_test.go`: `Barrier`, `Send`, and `Recv` tests.
- `errors_test.go`: error wrapping and error-context tests.
- `fake_test.go`: in-memory backend used by public package tests.
- `integration_test.go`: hardware integration tests. Safe discovery/failure
  checks require `JACCL_TEST_RDMA=1`; tests that transition queue pairs to RTR
  also require `JACCL_TEST_RDMA_ALLOW_RTR=1`.
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
  request posting, completion polling, context checks, and hybrid backoff.
- `rdma_test.go`: loader, device, queue, memory, work request, polling, and
  purego-boundary tests.

This package must not use cgo. Incorrect generated binding details should be
fixed in the generator or binding source, not by hand-editing generated files.
If `rdma_darwin_arm64.go` becomes too large to navigate after implementation,
split it by measured subsystem size, not before.

## Files Not To Add Yet

- Do not add `go.sum` directly.
- Do not add a `pkg/` tree.
- Do not add public `mesh`, `ring`, `dtype`, `rdma`, or `tcpchan` packages.
- Do not add `Float16` or `BFloat16` public files until parity tests against
  C++ JACCL semantics exist.
- Do not add command packages or examples directories until the package API is
  implemented and the tests can run real examples.
