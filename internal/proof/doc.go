// Package proof contains local helpers for operator proof packets.
//
// The package does not open RDMA devices, start jaccld, connect to peers, or
// run hardware probes. Commands that perform those actions must keep explicit
// operator confirmation and no-retry boundaries at their entry points.
package proof
