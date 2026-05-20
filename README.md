# gojaccl

gojaccl is a Go implementation of the JACCL collective communication model for
Apple RDMA over Thunderbolt. It provides a small Go API, a daemon-backed
resource owner for macOS Thunderbolt RDMA, and operator proof packets for the
hardware paths that require explicit physical validation.

The public package is `jaccl`. It exposes a small synchronous API around a live
communication group:

- `NewGroup` and `NewGroupFromEnv` initialize a rank.
- `Barrier`, `Send`, and `Recv` provide control and byte-oriented point-to-point
  operations.
- `NewSendWriter` and `NewRecvReader` expose point-to-point traffic as
  `io.Writer` and `io.Reader` streams.
- `AllSum`, `AllMax`, `AllMin`, and `AllGather` provide typed collectives over
  the supported `Element` types.

The implementation keeps RDMA details internal. Callers pass ordinary Go
slices; the backend copies through persistent mmap-backed staging buffers and
does not register caller heap memory in the hot path.

Streaming uses the standard library:

```go
w, err := g.NewSendWriter(ctx, 1)
if err != nil {
	return err
}
if _, err := io.Copy(w, file); err != nil {
	_ = w.Close()
	return err
}
return w.Close()
```

The receiving rank uses `NewRecvReader` and `io.Copy`. Use `bufio.NewReader`
or `bufio.NewWriter` around the returned values when buffering is useful.

`cmd/jaccld` is the daemon path for macOS Thunderbolt RDMA resource ownership.
It keeps the device, protection domain, and global registered slab in one
process and serves local clients over a Unix-domain socket. The daemon IPC
protocol leases and maps the slab, exposes explicit resource session leases,
then asks the daemon-owned RDMA transport to send, receive, or synchronize over
slab offsets. Daemon-backed collectives submit asynchronous work and wait for
completion over the same control connection. The IPC surface also has an
explicit maintenance request for the gated data-QP maintenance operation; it is
not a background keepalive.

Backend selection is explicit. Empty or `auto` uses the current direct backend;
`direct` selects it intentionally. `daemon` selects the IPC client backend for
barrier, point-to-point operations, and daemon-supported collectives.
Daemon clients can be configured with only rank, size, and daemon socket; they
do not need the direct backend coordinator or RDMA device matrix.

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

To run a physical test, start one `TestIntegrationChild` process per host with
`JACCL_TEST_RDMA_CHILD=1` and `JACCL_TEST_RDMA_ALLOW_RTR=1`, distinct
`JACCL_TEST_RANK` values, and the same reachable `JACCL_TEST_COORDINATOR`.
The canonical topology input is an explicit JSON device matrix in
`JACCL_TEST_RDMA_DEVICES`. That matrix may describe a complete mesh, or a sparse
connected topology supported by the backend. The legacy single-device shorthand
`JACCL_TEST_RDMA_DEVICE` is accepted only for two-rank integration helpers,
because it expands to a complete matrix. Use `JACCL_TEST_RDMA_DEVICES` for every
three-or-more-rank attempt. A sparse three-host line should leave the
endpoint-to-endpoint entries empty in the matrix; no additional topology flag is
required.

Before any physical three-or-more-rank attempt, validate the matrix offline:

```sh
go run ./cmd/jacclproof topology -file devices.json
```

This prints selected topology, rank count, directed and empty edge counts, wire
counts, and the matrix SHA-256. It does not open RDMA devices, start a
coordinator, allocate queue pairs, or authorize RTR.

macOS Thunderbolt RDMA provider failures can leave uninterruptible processes, so
do not run the RTR gate casually.

For the current two-M4 setup, do not model a missing third host. Generate an
explicit two-rank matrix for the cables that are physically connected:

```sh
go run ./cmd/jacclproof devices \
  -ranks 2 \
  -devices rdma_en1,rdma_en3 \
  > /tmp/gojaccl-two-m4-devices.json

go run ./cmd/jacclproof topology -file /tmp/gojaccl-two-m4-devices.json
```

If the attached cable maps to different RDMA device names on the two hosts,
override the directed rows instead of forcing a symmetric name. For example, if
rank 0 uses `rdma_en3` and rank 1 uses `rdma_en2`:

```sh
go run ./cmd/jacclproof devices \
  -ranks 2 \
  -edge 0,1=rdma_en3 \
  -edge 1,0=rdma_en2 \
  > /tmp/gojaccl-two-m4-devices.json
```

The topology report lists both `devices` and `primary_devices`. The direct
backend currently opens the first usable device listed for each peer edge, so a
dual-cable matrix is useful topology evidence but not a claim that both cables
carried datapath traffic in one run. Use separate metadata packets for each
device and keep the reviewed soak on the explicit `rdma_en1` proof path unless
a new proof envelope is written.

The current hardware proof mode starts a static daemon rank with explicit rank
metadata:

```sh
jaccld -rank 0 -size 2 -coordinator 127.0.0.1:9000
```

That is not the final once-per-boot production topology model. Production
`jaccld` should start once per host boot with hardware and IPC options, then
create topology sessions dynamically after startup. Until rank, size, and peer
selection move out of startup, the daemon-backed physical claim remains limited
to the accepted static two-host `rdma_en1` artifacts.

The default production control plane uses loopback `tcpchan` addresses,
normally through SSH local forwards between hosts. Non-loopback coordinators are
rejected unless `-allow-remote-tcpchan` is set after an explicit `jacclctl
tcp-diagnostic` proof. Direct non-loopback `tcpchan` is currently proven only
for the documented two-host `rdma_en1` IP pair.

Before attempting a new hardware path, collect provider metadata without moving
any queue pair to RTR:

```sh
jacclctl rdma-metadata -device rdma_en1 -max-gids 1024
jacclctl rdma-metadata -device rdma_en3 -max-gids 64
```

This opens the device and queries port/GID metadata only. It does not allocate
PDs, MRs, CQs, or QPs, and it does not post work requests.

For cross-host evidence, use the `jacclproof` packet command instead of ad hoc
commands:

```sh
go run ./cmd/jacclproof rdma-metadata \
  -device rdma_en1 \
  -remote <peer-ssh> \
  -remote-tmp <peer-tmp-dir> \
  -expected-selected-gid-index <expected-gid-index>
```

The command preserves a timestamped artifact under `~/tmp` and still does not
authorize RTR. Its final evaluator only classifies metadata collection.

The next no-RTR preflight is allocation-only:

```sh
go run ./cmd/jacclproof rdma-alloc \
  -device rdma_en2 \
  -remote-device rdma_en3 \
  -remote <peer-ssh> \
  -remote-tmp <peer-tmp-dir>
```

This packet allocates and tears down a protection domain, memory region,
completion queue, and queue pair on each host. It does not transition the queue
pair to RTR and does not post work requests.

After allocation passes, an INIT-only packet can prove the first local QP state
transition without crossing into RTR:

```sh
go run ./cmd/jacclproof rdma-init \
  -device rdma_en2 \
  -remote-device rdma_en3 \
  -remote <peer-ssh> \
  -remote-tmp <peer-tmp-dir>
```

RTR, RTS, and datapath work requests remain separate hardware gates.

An operator can trigger the explicit maintenance operation through the daemon
socket:

```sh
jacclctl maintain -timeout 5s
```

The reviewed one-shot hardware packet is `jacclproof rdma-soak`. It runs the
safe gates, metadata packet, direct TCP diagnostic, supervised daemons, pre/post
smoke, 60-second maintenance cadence, stats captures, postflight, cleanup, and
artifact packaging. It refuses without
`CONFIRM_RDMA_EN1_SOAK_ONE_SHOT=one-shot-soak`.

Operators can inspect daemon resource leases and jaccld-observed provider slot
counters without touching RDMA hardware:

```sh
jacclctl stats
```

The slot ledger is scoped to the current OS boot and to resources allocated by
`jaccld` itself. It reports protection-domain, memory-region, queue-pair, and
completion-queue opens, close calls, failures, outstanding opens, and resources
live in the current daemon process. It does not claim to see slots consumed by
unrelated processes.

Daemon-backed integration tests use the same `TestIntegrationChild` helper as
the direct backend, with `JACCL_BACKEND=daemon` and `JACCL_DAEMON_SOCKET` set to
the local daemon socket for each rank. Custom daemon socket paths must be placed
in an owner-only directory.

`-no-rdma` starts only the IPC server and slab allocator for hardware-free
smoke tests.

Daemon-backed RDMA_WRITE heartbeats are disabled by default and are not the
production keepalive path on Apple Thunderbolt RDMA, whose observed registered
memory has remote key zero. Background same-data-QP SEND/RECV heartbeats are
also rejected because receive matching is remote FIFO, and WR IDs are local
completion metadata, not wire tags.

The accepted production envelope is explicit same-data-QP maintenance, not a
background heartbeat: two Apple Thunderbolt RDMA hosts, RDMA pinned to
`rdma_en1`, admission stopped on all ranks, peer locks held, side-channel
pre/post barriers, and fail-closed route poisoning on any provider, CQ, barrier,
or maintenance error. This path has preserved long-idle proofs with passing
pre/post daemon-backed barrier-sum using both SSH-forwarded loopback `tcpchan`
and direct non-loopback `tcpchan` over the `rdma_en1` IP pair. Current HEAD is
not automatically physically proven by those artifacts. The last physically
proven commit, `1c96ef3ea5dee212f19fd2a67d5ef53d943bdf76`, has an accepted
2-hour soak: 120/120 maintenance rounds per rank, post-soak smoke passing,
postflight `rdma_en1` active on both hosts, and cleanup clean.

The proof artifacts are evidence for that exact deployment envelope and captured
binary. Later commits that affect provider setup, transport behavior, daemon
lifecycle, proof commands, maintenance semantics, stream adapters, or workload
proof surfaces require a fresh bounded `rdma_en1` proof before that exact binary
can be called physically proven. RDMA_WRITE heartbeat production readiness,
arbitrary rank counts, non-`rdma_en1` layouts, and arbitrary non-loopback
deployments remain excluded until separately proven.

## Dependency

This module depends on the released `github.com/tmc/apple` module. Local
experiments may use an uncommitted workspace or command-line module override,
but committed module metadata must stay portable.

## Documents

Design and validation artifacts live under `docs/`:

- `docs/go-jaccl-spec.md`
- `docs/go-package-files.md`
- `docs/jaccld.md`
- `docs/jaccld-dynamic-topology.md`
- `docs/jaccld-keepalive.md`
- `docs/jaccld-data-qp-keepalive.md`
- `docs/operator-runbook.md`

Generated transcripts, timestamped proof packets, and local audit closeouts are
kept out of git. Preserve those under the artifact directory for the specific
run.
