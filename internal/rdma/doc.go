// Package rdma wraps the Apple RDMA verbs used by jaccl.
//
// This package is the boundary between jaccl's communicator logic and the
// generated github.com/tmc/apple/rdma bindings. It owns device discovery,
// protection domains, completion queues, queue pairs, memory registration,
// work requests, and completion polling.
//
// Registered memory used by jaccl operations is mmap-backed staging memory
// owned by the backend. The public package must not expose memory registration
// or require callers to pass registered buffers.
//
// The package must not use cgo. Missing or incorrect generated binding details
// should be fixed in the generator and regenerated, not hand-edited here.
package rdma
