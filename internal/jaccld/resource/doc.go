// Package resource defines jaccld's daemon-owned resource lease model.
//
// jaccld owns fragile RDMA resources for the life of the daemon process.
// Request handlers lease from resource pools; they do not open devices,
// allocate protection domains, register memory, or create queue pairs.
//
// The package is provider-free. It describes peer sessions, memory-region
// windows, queue-pair handles, and daemon state without importing the RDMA
// transport implementation. Hardware-backed pools can implement these
// interfaces later, after daemon startup has created the bounded resources.
package resource
