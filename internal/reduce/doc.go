// Package reduce maps jaccl element types to local reduction kernels.
//
// The package implements the local sum, max, and min operations used while
// moving data through RDMA staging buffers. Its type dispatch must match the
// public jaccl Element constraint and the dtype semantics of the C++ JACCL
// backend.
package reduce
