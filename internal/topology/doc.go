// Package topology validates jaccl device connectivity.
//
// The input is the rank-by-rank device matrix used by MLX JACCL. A valid mesh
// has one usable device path for every non-self peer. A valid ring has matching
// adjacent links and may contain multiple wires per neighbor.
package topology
