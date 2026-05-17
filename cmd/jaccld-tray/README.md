# jaccld-tray

`jaccld-tray` shows `jaccld` resource leases and slot counters in the macOS menu
bar. It reads the existing `session_stats` IPC request and does not touch RDMA
provider setup.

Run it against a daemon socket:

```sh
go run ./cmd/jaccld-tray -socket /tmp/jaccld/run/jaccld-dev.sock
```

The menu title is `J0` when the daemon is reachable with no live provider slots,
`J<n>` when `jaccld` reports live slots, `Jo<n>` when boot-scoped outstanding
slots remain, and `J!` when the daemon cannot be reached.
