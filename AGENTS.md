# Agent Guidelines

- Write Go in the style of the standard library: small APIs, explicit errors,
  simple control flow, and no panics for expected failures.
- Keep the public package focused on ranks, groups, collectives, point-to-point
  operations, configuration, and errors.
- Keep RDMA details internal. Do not expose queue pairs, protection domains,
  memory regions, device handles, or TCP side-channel messages from package
  `jaccl`.
- Do not use cgo. The RDMA binding boundary is purego through
  `github.com/tmc/apple/rdma`.
- Do not register caller Go heap slices for RDMA operations. Copy through
  backend-owned mmap-backed staging memory.
- Do not run macOS RTR-driving RDMA tests unless the user explicitly asks for
  that hardware gate. `JACCL_TEST_RDMA_ALLOW_RTR=1` is a manual one-shot gate.
- Never write `go.sum` directly; let Go tooling update it.
- When writing package documentation, follow https://go.dev/doc/comment.
- Do not stage binary files to git.

## Commit Guidelines

- Keep commits narrow and reviewable.
- Prefer Go-style subjects: short, imperative, and scoped when useful.
- Preserve unrelated parent-worktree state. This directory is its own Git repo.
