# Findings for TinyKV Project 2

## Initial orientation

- Project 2 has three parts: Raft election/log/RawNode (2A), replicated KV over Raft (2B), and log GC plus snapshots (2C).
- Current user changes are in `kv/raftstore/peer_msg_handler.go` and `kv/raftstore/peer_storage.go`; preserve them and validate their behavior rather than replacing them wholesale.
- Explicit Project 2 TODOs initially visible:
  - `raft/log.go`: `maybeCompact` (2C)
  - `raft/raft.go`: `handleSnapshot` (2C)
  - `kv/raftstore/peer_storage.go`: `ApplySnapshot` (2C)
  - 2B skeleton markers remain in the modified peer files and require test validation.
- Project 3 TODOs in `raft/raft.go` are out of scope.

## Key contracts

- Raft emits `Ready`; the application must persist state/entries before sending messages, apply committed entries, then call `Advance`.
- `PeerStorage` exposes persisted Raft log/state through `raft.Storage` and separates `raftdb` from the KV state machine in `kvdb`.
- Applying a command must persist the KV mutation and `RaftApplyState.AppliedIndex` together.
- Proposal callbacks are matched by log index and term so overwritten proposals can receive stale-command errors.

## Baseline

- `go test ./raft -run '2A' -count=1` passes. The existing 2A implementation is not the immediate failure source.
- `Makefile`'s `project2b` and `project2c` recipes contain `|| true`, so those targets cannot be used as the sole pass/fail signal; run direct `go test` commands or inspect each test result.
- Direct `TestBasic2B` passes with the current user changes.
- Existing `kv/raftstore` unit tests pass, including `PeerStorage.Append` and storage range/term behavior.
- Raft 2C currently fails in `TestRestoreSnapshot2C`: `handleSnapshot` is an explicit TODO, and the log remains at index 0 instead of the snapshot index 11.
- 2C wiring is also incomplete beyond the explicit handler: `Step` has no `MsgSnapshot` case, and `sendAppend` currently returns false when the previous index is compacted without enqueuing a snapshot message.
- Snapshot application must bridge two layers: Raft metadata (`pendingSnapshot`, dummy log entry, commit/applied boundaries, peer set) and raftstore state-machine data (`RegionTaskApply`, `RaftLocalState`, `RaftApplyState`, `RegionLocalState`).

## Current Raft 2C status

- Raft-side 2C now passes all direct tests.
- A subtle send-path bug was fixed: an unavailable previous-log term is expected when a follower is behind compaction; it must not be reused as the error for a successfully selected snapshot.

## RaftStore snapshot implementation

- `PeerStorage.ApplySnapshot` clears old metadata only for an initialized peer, resets `RaftLocalState` and `RaftApplyState` at the snapshot boundary, persists the new RegionLocalState, and waits for the region worker to ingest the received snapshot before Ready is advanced.
- The worker receives the previous key range for cleanup and the snapshot metadata for locating the received SST files.
- The single-node end-to-end `TestOneSnapshot2C` passes after this bridge was added.

## Final status

- Project 2 implementation is complete in the current worktree.
- Raft 2A/2B/2C direct tests and `kv/raftstore` unit tests pass.
- The final unreliable concurrent-partition snapshot-recovery integration test passes after rejecting stale AppendEntries that precede the compacted in-memory log boundary.
- Project 3 membership/split TODOs remain intentionally untouched.
