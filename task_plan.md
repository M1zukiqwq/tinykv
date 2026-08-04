# TinyKV Project 2 completion plan

## Goal

Complete Project 2 (RaftKV) in the current worktree, preserve existing user changes, and verify the Project 2A/2B/2C tests.

## Phases

- [completed] Phase 1: Inspect current changes, remaining TODOs, contracts, and establish test baseline.
- [completed] Phase 2: Complete or repair Raft 2A/RawNode behavior if baseline exposes gaps.
- [completed] Phase 3: Complete or repair RaftStore 2B persistence, proposal, Ready, apply, and callback behavior.
- [completed] Phase 4: Complete Raft/raftstore 2C log compaction and snapshot generation/application/transport behavior.
- [completed] Phase 5: Run narrow tests, then the complete Project 2 suite; summarize changed functionality and residual issues.

## Scope constraints

- Preserve unrelated existing modifications and AGENTS.md.
- Do not implement Project 3 membership/split/leader-transfer behavior unless required to compile or pass Project 2 tests.
- Use the existing architecture and protobuf contracts; avoid broad refactors.

## Success criteria

- `make project2aa`, `make project2ab`, `make project2ac`, and `make project2a` pass.
- Project 2B mock-cluster tests pass.
- Project 2C Raft and snapshot/recovery tests pass.
- Formatting and focused static checks pass for changed Go files.

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| `TestRestoreSnapshot2C` reports `log.lastIndex = 0, want 11` and then panics on unavailable term | 1 | Do not rerun unchanged; inspect and implement Raft snapshot restore path. |
| `TestSnapshotUnreliableRecoverConcurrentPartition2C` panics from an unsigned log slice bound in `handleAppendEntries` after snapshot recovery | 1 | Reject stale/malformed entries at or before the snapshot boundary before truncating the in-memory log. |

## Final verification

- Raft 2A/2B/2C direct tests pass after the final AppendEntries guard.
- `kv/raftstore` unit tests pass.
- The complete `go test -v ./kv/test_raftstore -run '2[BC]$' -count=1` run passes all Project 2B/2C scenarios, including the high-pressure snapshot recovery case.
- `git diff --check` passes.
