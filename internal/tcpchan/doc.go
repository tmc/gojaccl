// Package tcpchan implements jaccl's TCP side channel.
//
// The side channel exchanges rank metadata and RDMA destination data before
// queue pairs are ready for RDMA traffic. It also provides the initialization
// barriers needed to keep ranks from advancing before every peer has published
// its destination information.
package tcpchan
