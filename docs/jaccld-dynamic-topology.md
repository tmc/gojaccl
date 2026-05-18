# jaccld dynamic topology

`jaccld` should be a boot-scoped hardware owner. A production host should start
one daemon for the OS boot, open the RDMA device, allocate the protection
domain, register the slab, expose IPC, and keep scarce-slot accounting alive
independently of any single collective topology.

The current hardware-proof path is narrower. It starts one daemon rank with
`-rank`, `-size`, and `-coordinator`, forms a fixed two-rank daemon transport at
startup, and then serves local clients through that transport. That shape is
acceptable as a proof harness for the accepted `rdma_en1` envelope, but it is
not the final production topology model.

## Target Process Model

Boot-scoped daemon startup should take only host-local hardware and IPC
configuration:

```sh
jaccld -socket /tmp/jaccld/jaccld.sock -device rdma_en1
```

Startup owns:

- RDMA device open;
- one protection domain;
- one registered slab memory region;
- owner-only Unix-domain socket;
- per-boot scarce-slot ledger;
- provider-free resource store and liveness bookkeeping.

Startup should not require:

- group rank;
- group size;
- collective coordinator;
- fixed peer set;
- one queue-pair graph for all future clients.

Those values belong to topology sessions created after daemon startup.

## Topology Sessions

A topology session describes one logical collective group. It is distinct from a
resource lease. A session should include:

- session ID and epoch;
- rank count;
- local rank;
- rank-to-node map;
- peer daemon identities;
- side-channel addresses or peer-registry references;
- deadline and refresh policy;
- admission state;
- maintenance policy for the data QPs used by that session.

Creating a topology session may allocate or reuse daemon-owned QP/CQ resources,
but the allocation still happens inside `jaccld`, not in the client process.
Closing or expiring a session releases logical leases and route handles without
tearing down the boot-scoped device, protection domain, or registered slab.

## Peer Mesh

The daemon should maintain a peer registry separate from a group:

- local node ID and boot ID;
- peer node ID;
- peer control address;
- device and GID metadata;
- last liveness observation;
- route health and proof scope;
- supported transport capabilities.

The peer mesh can be updated as machines appear, disappear, or change routes.
Admission for a topology session then selects from the current peer registry.
The planner may request a group of any supported size, but `jaccld` should fail
closed if the peer registry cannot supply proved, healthy routes for that
topology.

## IPC Choice

The primary local control plane should remain a Unix-domain socket. The daemon
needs local credentials, owner-only filesystem permissions, cancellation, small
request/response messages, and `SCM_RIGHTS` file-descriptor passing so clients
can map the daemon-owned slab without copying it through the control protocol.
Those are native UDS strengths.

9P is not the right primary interface for the hot path. It is a filesystem
protocol, so it is attractive for browsing status, logs, manifests, and maybe a
read-only operator namespace. It does not naturally model collective admission,
deadline-bound topology sessions, completion polling, or native macOS file
descriptor passing for the registered slab.

A future admin facade could expose read-only state as files, but the slab map,
lease, send, recv, collective, and maintenance operations should stay on UDS
unless a separate design proves that another local IPC keeps the same
fail-closed semantics and zero-copy mapping behavior.

## Current Static Mode

The existing `-rank`, `-size`, and `-coordinator` flags are the current static
transport mode. They remain useful for accepted proof packets because they make
the hardware experiment explicit and bounded. Do not mistake them for the
production boot daemon API.

Future code should either:

- move these values into an IPC topology-session request; or
- rename the startup path as an explicit static/proof mode before claiming
  general production readiness.

Until then, the production claim remains limited to the accepted two-host
`rdma_en1` static topology artifacts.
