// Package jaccl provides collective communication over Apple RDMA.
//
// A Group is a live communicator. It reports the local rank and group size,
// runs blocking collectives with context cancellation, and releases RDMA
// resources with Close.
//
// Configuration is explicit. Config records the local rank, the rank-zero
// coordinator address, the rank-by-rank RDMA device matrix, and whether a valid
// ring topology should be preferred.
//
// NewGroup validates the configuration, creates the side channel, initializes
// RDMA resources, and returns only after the group is ready. NewGroupFromEnv
// reads the JACCL_RANK, JACCL_IBV_DEVICES, JACCL_COORDINATOR, and JACCL_RING
// environment variables, with MLX_* fallbacks matching MLX JACCL.
//
// Errors include operation and rank context when available.
//
// Collectives such as AllSum and AllGather are generic functions constrained
// to supported Element types. Send and Recv move opaque byte slices for
// point-to-point traffic.
//
// Collective functions validate slice lengths before posting RDMA work. AllSum,
// AllMax, and AllMin require dst and src to have the same length. They support
// in-place operation when dst and src are the same slice. AllGather requires
// len(dst) == g.Size()*len(src).
//
// The RDMA backend copies through internal registered staging buffers. Callers
// do not need to provide page-aligned or registered memory.
//
// Float16 and BFloat16 are deferred until the Go implementation has explicit
// types and parity tests for the C++ JACCL semantics. Element accepts only
// predeclared numeric types for now, so named uint16 aliases cannot silently
// opt into half-precision wire semantics.
//
// Collectives must be called in the same order on every rank. A single Group
// permits at most one active collective or point-to-point operation at a time.
//
// The RDMA backend is darwin/arm64 using the purego github.com/tmc/apple/rdma
// bindings. Other platforms report that RDMA is unavailable.
package jaccl
