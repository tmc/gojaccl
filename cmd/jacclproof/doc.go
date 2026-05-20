// Command jacclproof runs explicit operator proof packet helpers.
//
// The devices, process-snapshot, and topology subcommands are local and
// provider-free. The rdma-metadata command collects bounded metadata evidence
// without RTR. The rdma-alloc command proves provider resource allocation and
// teardown without RTR or work requests. The rdma-init command additionally
// proves the local QP INIT transition while still stopping before RTR. The
// rdma-soak command owns the reviewed two-host rdma_en1 proof packet and
// refuses to run unless its one-shot confirmation environment variable is set.
package main
