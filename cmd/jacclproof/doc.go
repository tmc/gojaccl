// Command jacclproof runs explicit operator proof packet helpers.
//
// The process-snapshot subcommand is local and provider-free. The rdma-metadata
// command collects bounded metadata evidence without RTR. The rdma-soak command
// owns the reviewed two-host rdma_en1 proof packet. Hardware proof subcommands
// refuse to run unless the matching one-shot confirmation environment variable
// is set.
package main
