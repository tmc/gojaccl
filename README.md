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
protocol leases and maps the slab, exposes explicit resource session leases,
then asks the daemon-owned RDMA transport to send, receive, or synchronize over
slab offsets. Daemon-backed collectives submit asynchronous work and wait for
completion over the same control connection.

Backend selection is explicit. Empty or `auto` uses the current direct backend;
`direct` selects it intentionally. `daemon` selects the IPC client backend for
barrier, point-to-point operations, and daemon-supported collectives.

## Status

The non-hardware Go surface is implementation-ready:

```sh
CGO_ENABLED=0 go test ./...
```

Hardware RDMA tests are intentionally not part of ordinary validation on macOS.
Tests that transition queue pairs to RTR require explicit one-shot operator
confirmation and a real physical topology. The top-level integration tests do
not run a local loopback RTR experiment by default; Apple Thunderbolt RDMA is a
point-to-point link between hosts, not a same-host loopback fabric.

```sh
JACCL_TEST_RDMA=1 JACCL_TEST_RDMA_ALLOW_RTR=1 go test -run '^TestIntegration' .
```

To run a physical two-host test, start one `TestIntegrationChild` process per
host with `JACCL_TEST_RDMA_CHILD=1` and `JACCL_TEST_RDMA_ALLOW_RTR=1`,
distinct `JACCL_TEST_RANK` values, the same reachable
`JACCL_TEST_COORDINATOR`, and each host's local `JACCL_TEST_RDMA_DEVICE`.

macOS Thunderbolt RDMA provider failures can leave uninterruptible processes, so
do not run the RTR gate casually.

A daemon rank is started with explicit rank metadata:

```sh
jaccld -rank 0 -size 2 -coordinator 127.0.0.1:9000
```

Daemon-backed integration tests use the same `TestIntegrationChild` helper as
the direct backend, with `JACCL_BACKEND=daemon` and `JACCL_DAEMON_SOCKET` set to
the local daemon socket for each rank.

`-no-rdma` starts only the IPC server and slab allocator for hardware-free
smoke tests.

Daemon-backed RDMA heartbeats are disabled by default. The current production
path proves daemon-owned resource and data-path ownership, but not long-lived
idle-QP keepalive safety. The experimental RDMA-write heartbeat path requires
`-experimental-rdma-heartbeat`, a positive `-heartbeat-timeout`, and nonzero
remote heartbeat address and rkey metadata; it fails closed when that metadata
is missing.

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
