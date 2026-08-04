# Progress log

## 2026-08-02

- Started Project 2 completion work.
- Read TinyKV teacher, coding guardrail, and file-planning instructions.
- Confirmed the worktree has existing uncommitted changes in `.gitignore`, `kv/raftstore/peer_msg_handler.go`, `kv/raftstore/peer_storage.go`, and untracked `AGENTS.md`.
- Confirmed explicit remaining Project 2 TODOs in Raft log compaction, Raft snapshot handling, and `PeerStorage.ApplySnapshot`.
- Baseline `go test ./raft -run '2A' -count=1` passed.
- Project 2A is therefore treated as a protected working baseline while investigating 2B/2C.
- Direct `TestBasic2B` passed.
- Existing `kv/raftstore` unit tests passed.
- Direct Raft 2C baseline failed at `TestRestoreSnapshot2C` because `handleSnapshot` is unimplemented.
- Inspection showed the complete 2C send/receive path is incomplete: no `MsgSnapshot` dispatch, no snapshot fallback from `sendAppend`, no `RaftLog.maybeCompact`, and no raftstore `ApplySnapshot` implementation.
- Implemented and gofmt-checked the Raft-side compaction, pending-snapshot Ready/Advance handling, snapshot send fallback, snapshot receive handling, and message dispatch.
- Direct `go test -v ./raft -run '2C' -count=1` now passes all Raft 2C tests, including slow-node recovery and RawNode restart from snapshot.
- Implemented `PeerStorage.ApplySnapshot`: stale metadata/log cleanup, Raft/apply state reset, Region state persistence, region-worker snapshot installation, and snapshot result reporting.
- `kv/raftstore` unit tests still pass after the snapshot changes.
- `go test -v ./kv/test_raftstore -run '^TestOneSnapshot2C$' -count=1` passes, including snapshot generation, transfer, application, and restart recovery.
- The full Project 2B/2C run passed the first 13 scenarios but exposed a panic in the final unreliable-partition snapshot recovery scenario; the failing path was an unsigned slice underflow in `handleAppendEntries` when a stale entry preceded the snapshot boundary.
- Added a defensive stale/malformed AppendEntries rejection before in-memory log truncation; the final scenario must be rerun to validate recovery through the rejection/snapshot retry path.
- Final rerun of `TestSnapshotUnreliableRecoverConcurrentPartition2C` passes after the guard.
- Final direct Raft 2A/2B/2C tests, `kv/raftstore` unit tests, and `git diff --check` pass.
- The complete `go test -v ./kv/test_raftstore -run '2[BC]$' -count=1` run passes all Project 2B/2C scenarios in 469.308s.
- Project 2 is complete; Project 3 TODOs remain out of scope.
