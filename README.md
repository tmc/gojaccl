# gojaccl

gojaccl is a Go implementation sketch of JACCL, the MLX collective
communication library for Apple RDMA over Thunderbolt.

The public package is `jaccl`. It exposes a small synchronous API around a live
communication group:

- `NewGroup` and `NewGroupFromEnv` initialize a rank.
- `Barrier`, `Send`, and `Recv` provide control and byte-oriented point-to-point
  operations.
- `AllSum`, `AllMax`, `AllMin`, and `AllGather` provide typed collectives over
  the supported `Element` types.

The implementation keeps RDMA details internal. Callers pass ordinary Go
slices; the backend copies through persistent mmap-backed staging buffers and
does not register caller heap memory in the hot path.

`cmd/jaccld` is the daemon path for macOS Thunderbolt RDMA resource ownership.
It keeps the device, protection domain, and global registered slab in one
process and serves local clients over a Unix-domain socket. The daemon IPC
protocol leases and maps the slab, then asks the daemon-owned RDMA transport to
send, receive, or synchronize over slab offsets. Daemon-backed collectives are
still a separate integration step because the current IPC protocol is
synchronous and intentionally does not multiplex concurrent work.

Backend selection is explicit. Empty or `auto` uses the current direct backend;
`direct` selects it intentionally. `daemon` selects the IPC client backend for
barrier and point-to-point operations. Daemon-backed collectives fail with a
clear unsupported error until the transport-neutral collective layer is wired.

## Status

The non-hardware Go surface is implementation-ready:

```sh
CGO_ENABLED=0 go test ./...
```

Hardware RDMA tests are intentionally not part of ordinary validation on macOS.
Tests that transition queue pairs to RTR require explicit one-shot operator
confirmation:

```sh
JACCL_TEST_RDMA=1 JACCL_TEST_RDMA_ALLOW_RTR=1 go test -run '^TestIntegration' .
```

macOS Thunderbolt RDMA provider failures can leave uninterruptible processes, so
do not run the RTR gate casually.

A daemon rank is started with explicit rank metadata:

```sh
jaccld -rank 0 -size 2 -coordinator 127.0.0.1:9000 -heartbeat 1m
```

`-no-rdma` starts only the IPC server and slab allocator for hardware-free
smoke tests.

The daemon reserves one byte in the shared slab and exchanges the registered MR
address and rkey with peer daemons. Idle heartbeats use RDMA write to that
reserved byte so they do not consume peer receive work requests or corrupt user
payloads.

## Dependency

This module currently uses a local `replace` for `github.com/tmc/apple` because
the required generated RDMA binding surface is not in the published
`github.com/tmc/apple` tags available in this workspace.

## Documents

Design and validation artifacts live under `docs/`:

- `docs/go-jaccl-spec.md`
- `docs/go-package-files.md`
- `docs/go-doc-output.txt`
- `docs/go-test-output.txt`
- `docs/jaccld.md`
