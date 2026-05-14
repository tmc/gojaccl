# jaccld operator runbook

This runbook describes the supported Apple Thunderbolt RDMA production envelope
proved for `jaccld`: two physical hosts, RDMA pinned to `rdma_en1`, daemon-owned
resources, and explicit same-data-QP maintenance during idle. The control plane
is proven both with SSH-forwarded loopback `tcpchan` and, for the documented
`rdma_en1` IP pair only, direct non-loopback `tcpchan`.

## Preconditions

- Use two physical Apple hosts connected by Thunderbolt RDMA.
- Use the same `gojaccl` binary hash on both hosts.
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
ssh -N -L 127.0.0.1:38411:127.0.0.1:38412 tmc2@10.0.18.249
```

Use matching `-coordinator 127.0.0.1:PORT` values when starting the two daemon
ranks. The daemon rejects non-loopback coordinators by default.

Direct non-loopback `tcpchan` has also been proved for the two-host `rdma_en1`
IP pair, using `172.31.253.2` as the rank-zero coordinator and explicit
`-allow-remote-tcpchan` on both daemon ranks. Before using any other
non-loopback path, prove payload delivery with `jacclctl tcp-diagnostic` and
preserve the output. Start the listener on one host:

```sh
jacclctl tcp-diagnostic -listen 10.0.18.249:39000
```

Dial from the other host:

```sh
jacclctl tcp-diagnostic -dial 10.0.18.249:39000
```

Only after that direct TCP diagnostic passes should an operator consider
`jaccld -allow-remote-tcpchan`. Outside the documented `rdma_en1` IP pair, this
still does not upgrade the production claim without a full RDMA proof using that
control plane.

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
  -coordinator 172.31.253.2:39311 \
  -allow-remote-tcpchan
```

Expected logs include side-channel startup, slab creation, hardware open,
protection-domain allocation, memory registration, daemon transport setup, and
IPC listen phases. Do not proceed if startup logs show a zero rkey as an
RDMA_WRITE heartbeat candidate; RDMA_WRITE is not the production path on Apple
Thunderbolt RDMA.

## Smoke And Maintenance

Run a daemon-backed datapath smoke before idle. The accepted proof used the
integration child harness with `JACCL_BACKEND=daemon` and a local
`JACCL_DAEMON_SOCKET` for each rank.

During idle, trigger explicit maintenance at a bounded interval:

```sh
jacclctl -socket /tmp/jaccld-rank0.sock maintain
jacclctl -socket /tmp/jaccld-rank1.sock maintain
```

Capture daemon logs showing `jaccld maintenance ... ok=true` for each peer.
Stop immediately on `ok=false`, poison, timeout, unexpected completion, barrier
failure, provider error, or CQ error. Do not retry the proof after such a
failure.

After the idle window, run the daemon-backed datapath smoke again.

## Postflight

Run provider checks on both hosts after the proof:

```sh
rdma_ctl status
ibv_devinfo -d rdma_en1
```

The provider must still report `rdma_en1` active. Record exact commands,
stdout, stderr, statuses, binary hashes, daemon logs, SSH forward logs,
maintenance counters, preflight state, postflight state, and the explicit
no-retry statement in a timestamped artifact directory under `~/tmp`.

The accepted proof artifact for the current envelope is:

```text
/Users/tmc/tmp/gojaccl-jaccld-dataqp-maintenance-proof-sshchan-20260514T090333Z
/Users/tmc/tmp/gojaccl-jaccld-dataqp-maintenance-proof-sshchan-20260514T090333Z.tar.gz
sha256 fd36e9726440a1224fafc9890184bbbc5321c114c3390baca25c2c7d2c054c67
```
