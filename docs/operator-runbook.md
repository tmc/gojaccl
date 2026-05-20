# jaccld operator runbook

This runbook describes the supported Apple Thunderbolt RDMA production envelope
proved for `jaccld`: two physical hosts, RDMA pinned to `rdma_en1`, daemon-owned
resources, and explicit same-data-QP maintenance during idle. The control plane
is proven both with SSH-forwarded loopback `tcpchan` and, for the documented
`rdma_en1` IP pair only, direct non-loopback `tcpchan`.

The proof is tied to the binary and source state captured in the artifact.
Commit `1c96ef3ea5dee212f19fd2a67d5ef53d943bdf76` is physically proven for
this envelope, including the accepted 2-hour soak preserved in the local proof
artifact bundle. If the operator is using a later commit, first treat it as a
hardened candidate: run the safe no-hardware gates, then rerun the bounded
`rdma_en1` proof before claiming that exact binary is physically proven.

## Preconditions

- Use two physical Apple hosts connected by Thunderbolt RDMA.
- Use the same `gojaccl` binary hash on both hosts.
- Confirm the binary hash corresponds to the commit being claimed; post-proof
  hardening does not inherit physical proof without a fresh run.
- Do not use same-host RDMA loopback as datapath evidence.
- Do not retry automatically after provider, RTR, CQ, maintenance, barrier,
  poison, or postflight failure.
- Keep proof artifacts local if they include hostnames, socket paths, or SSH
  forward details.

## Preflight

Run provider checks on both hosts before starting `jaccld`:

```sh
rdma_ctl status
ibv_devinfo -d rdma_en1
```

The port used by the proof was `rdma_en1` and had to be active before daemon
startup. If the provider is not active, stop before launching daemon ranks.

### Two M4s with two Thunderbolt cables

This is a two-rank proof shape. Do not synthesize a third rank or reuse the
three-host line plan when only two physical hosts are present.

Generate the candidate device matrix before collecting hardware evidence:

```sh
go run ./cmd/jacclproof devices \
  -ranks 2 \
  -devices rdma_en1,rdma_en3 \
  > /tmp/gojaccl-two-m4-devices.json

go run ./cmd/jacclproof topology -file /tmp/gojaccl-two-m4-devices.json
```

The topology command is provider-free. Its `devices` field records every device
named in the matrix, while `primary_devices` records the first usable device on
each directed edge. The direct backend opens that first device per peer edge.
Therefore a two-cable matrix is evidence that the operator intended both cables
to be present, not proof that one datapath run used both cables.

Collect metadata for both cable-backed RDMA devices before any RTR attempt:

```sh
CONFIRM_RDMA_EN1_METADATA_ONE_SHOT=one-shot-metadata \
  go run ./cmd/jacclproof rdma-metadata \
    -device rdma_en1 \
    -remote <peer-ssh> \
    -remote-tmp <peer-tmp-dir> \
    -expected-selected-gid-index 1

CONFIRM_RDMA_EN3_METADATA_ONE_SHOT=one-shot-metadata \
  go run ./cmd/jacclproof rdma-metadata \
    -device rdma_en3 \
    -remote <peer-ssh> \
    -remote-tmp <peer-tmp-dir>
```

Treat `rdma_en3` as metadata-only until a separate reviewed datapath packet is
added. The reviewed long-idle datapath packet remains `rdma-soak` on
`rdma_en1`.

For any new hardware path, collect provider port and GID metadata before any
RTR attempt:

```sh
jacclctl rdma-metadata -device rdma_en1 -max-gids 1024
jacclctl rdma-metadata -device rdma_en3 -max-gids 64
```

This diagnostic opens the named device and queries port/GID metadata only. It
does not allocate a protection domain, memory region, completion queue, or queue
pair. `-max-gids` bounds the metadata scan; increase it only with a concrete
reason and the same outer timeout. Ordinary TCP reachability is not enough to
prove the provider can route an RDMA QP to RTR.

`rdma_en1` is the Apple RDMA provider device name, not necessarily a macOS
network interface name. Before assigning or checking the documented direct
`tcpchan` IPv4 pair, map the provider GID to the real macOS interface by MAC:

- read the nonzero `jacclctl rdma-metadata` GID;
- derive the EUI-64 MAC from the link-local GID;
- find that MAC in `ifconfig -a`, usually on the Thunderbolt bridge member
  (`en1`) and `bridge0`;
- verify the intended peer route with `route -n get`.

If `ifconfig rdma_en1` does not exist, do not treat that alone as failure or
success. The provider device name may differ from the macOS network-interface
name. Continue by mapping the provider GID MAC to the physical Thunderbolt
interface, then stop before RTR if the intended point-to-point route selects a
non-Thunderbolt path.

Apple TN3205 ties RDMA GIDs to IP addresses on the paired Thunderbolt IP
interface. On this host pair, assigning the documented IPv4 pair to `bridge0`
fixed ordinary OS routing and TCP but did not change provider GID metadata.
Assigning the pair to the mapped physical interface `en1` caused `rdma_en1` to
advertise IPv4-mapped GIDs at index 1:

- local mapped Thunderbolt interface: `<local-rdma-ip>/30`,
  `gid index=1 value=::ffff:<local-rdma-ip>`;
- peer mapped Thunderbolt interface: `<peer-rdma-ip>/30`,
  `gid index=1 value=::ffff:<peer-rdma-ip>`.

For this configuration, run metadata preflight with
`EXPECTED_SELECTED_GID_INDEX=1` and stop before daemon startup if either host
lacks the IPv4-mapped GID at index 1.

For the current post-reboot `rdma_en1` `errno 60` state, use the gated
metadata packet:

```sh
CONFIRM_RDMA_EN1_METADATA_ONE_SHOT=one-shot-metadata \
  go run ./cmd/jacclproof rdma-metadata \
    -device rdma_en1 \
    -remote <peer-ssh> \
    -remote-tmp <peer-tmp-dir> \
    -expected-selected-gid-index 1
```

This packet classifies metadata collection only. It is not a reproof packet and
does not authorize RTR.

Confirm both hosts will run the same binary:

```sh
shasum -a 256 ./jaccld ./jacclctl ./gojaccl.test
```

## TCP Side Channel

The default supported control-plane shape is loopback `tcpchan` over SSH local
forwards.

One host should forward a local coordinator port to the other host. Use
explicit loopback endpoints; do not tunnel RDMA payloads, daemon UDS traffic,
or provider state through SSH.

Example shape:

```sh
ssh -N -L 127.0.0.1:<local-port>:127.0.0.1:<peer-port> <peer-ssh>
```

Use matching `-coordinator 127.0.0.1:PORT` values when starting the two daemon
ranks. The daemon rejects non-loopback coordinators by default.

Direct non-loopback `tcpchan` has also been proved for the two-host `rdma_en1`
IP pair recorded in the proof artifact, using the peer RDMA IP as the
rank-zero coordinator and explicit
`-allow-remote-tcpchan` on both daemon ranks. Before using any other
non-loopback path, prove payload delivery with `jacclctl tcp-diagnostic` and
preserve the output. Start the listener on one host:

```sh
jacclctl tcp-diagnostic -listen <listener-ip>:39000
```

Dial from the other host:

```sh
jacclctl tcp-diagnostic -dial <listener-ip>:39000
```

Only after that direct TCP diagnostic passes should an operator consider
`jaccld -allow-remote-tcpchan`. Outside the documented `rdma_en1` IP pair, this
still does not upgrade the production claim without a full RDMA proof using that
control plane.

For the operator-supplied `rdma_en1` point-to-point IP pair, the route must
select the mapped physical Thunderbolt interface. Stop before daemon startup if
`route -n get <local-rdma-ip>` or `route -n get <peer-rdma-ip>` selects
`bridge0`, `en0`, a default gateway, Wi-Fi, LAN, or any interface other than
the mapped physical Thunderbolt interface.

## Daemon Startup

Start one daemon rank per host. Bind RDMA to `rdma_en1` and use either loopback
coordinator addresses from the SSH-forwarded side channel or the documented
direct `rdma_en1` coordinator address with `-allow-remote-tcpchan`.

Rank 0 example:

```sh
jaccld \
  -socket /tmp/jaccld-rank0.sock \
  -device rdma_en1 \
  -rank 0 \
  -size 2 \
  -coordinator 127.0.0.1:38412
```

Rank 1 example:

```sh
jaccld \
  -socket /tmp/jaccld-rank1.sock \
  -device rdma_en1 \
  -rank 1 \
  -size 2 \
  -coordinator 127.0.0.1:38411
```

Direct `rdma_en1` coordinator example:

```sh
jaccld \
  -socket /tmp/jaccld-rank0.sock \
  -device rdma_en1 \
  -rank 0 \
  -size 2 \
  -coordinator <rank0-rdma-ip>:39311 \
  -allow-remote-tcpchan
```

Expected logs include side-channel startup, slab creation, hardware open,
protection-domain allocation, memory registration, daemon transport setup, and
IPC listen phases. Do not proceed if startup logs show a zero rkey as an
RDMA_WRITE heartbeat candidate; RDMA_WRITE is not the production path on Apple
Thunderbolt RDMA.

Launch each daemon from a persistent supervised context, such as an interactive
iTerm session, tmux pane, or process supervisor. Do not treat a PID file from a
local background child of a short-lived non-interactive command as sufficient
daemon evidence.

After both daemons log `ipc_listen` and before running smoke, record:

- both daemon PIDs are alive;
- both expected Unix sockets exist;
- each daemon log is still free of panic, fatal, exit, provider, RTR, CQ,
  poison, and maintenance error lines after `ipc_listen`;
- each daemon's wait or supervisor exit-status file is present.

If either daemon has exited or liveness cannot be proven, stop before smoke,
preserve provider state and logs, clean up, and classify the artifact as
`STOPPED_AFTER_RTR_BEFORE_SMOKE_DAEMON_LIVENESS`. Do not restart daemons or
continue to smoke in that artifact.

## Smoke And Maintenance

Run a daemon-backed datapath smoke before idle. The accepted proof used the
integration child harness with `JACCL_BACKEND=daemon` and a local
`JACCL_DAEMON_SOCKET` for each rank.

During idle, trigger explicit maintenance at a bounded interval:

```sh
jacclctl -socket /tmp/jaccld-rank0.sock maintain -timeout 5s
jacclctl -socket /tmp/jaccld-rank1.sock maintain -timeout 5s
```

Capture daemon logs showing `jaccld maintenance ... ok=true` for each peer.
Stop immediately on `ok=false`, poison, timeout, unexpected completion, barrier
failure, provider error, or CQ error. Do not retry the proof after such a
failure.

After the idle window, run the daemon-backed datapath smoke again.

The reviewed one-shot soak packet is implemented as a Go command, not as an
in-repo shell script:

```sh
CONFIRM_RDMA_EN1_SOAK_ONE_SHOT=one-shot-soak \
  go run ./cmd/jacclproof rdma-soak \
    -remote <peer-ssh> \
    -remote-tmp <peer-tmp-dir> \
    -local-rdma-ip <local-rdma-ip> \
    -remote-rdma-ip <peer-rdma-ip> \
    -soak-seconds 7200
```

It runs the safe gates, builds and hashes both host binaries, runs the gated
metadata packet, checks direct TCP diagnostics, supervises both daemon ranks,
runs pre/post daemon-backed smoke, posts one maintenance round per rank every
60 seconds, captures stats and postflight state, and preserves a tarred
artifact under `~/tmp`. It remains a one-shot hardware proof; stop and do not
retry after any required gate fails.

## Postflight

Run provider checks on both hosts after the proof:

```sh
rdma_ctl status
ibv_devinfo -d rdma_en1
```

The provider must still report `rdma_en1` active. Record exact commands,
stdout, stderr, statuses, binary hashes, daemon logs, daemon liveness and
exit-status evidence, SSH forward logs, maintenance counters, preflight state,
postflight state, and the explicit no-retry statement in a timestamped artifact
directory under `~/tmp`.

The exact accepted proof artifact paths and hashes live in the preserved run
artifacts, not in reusable run commands. Keep timestamped proof packets and
local audit closeouts outside git unless they have been rewritten into durable
operator guidance.

The accepted direct-`tcpchan` proof used the artifact's recorded `rdma_en1`
point-to-point IP pair, persistent supervised daemon launch, post-IPC daemon
liveness/socket/log checks before smoke, pre/post daemon-backed `barrier-sum`
smoke, `maintenance-window2` with 45/45 counted rounds per rank at 60 second
cadence, postflight `enabled` / `PORT_ACTIVE` provider state on both hosts, and
empty cleanup snapshots.

The accepted 2-hour soak preserved the same direct `rdma_en1` control/data
envelope, ran 120/120 maintenance rounds per rank over 7200 seconds, passed
pre/post daemon-backed smoke, kept `rdma_en1` active in postflight, and cleaned
up with no proof processes remaining.
