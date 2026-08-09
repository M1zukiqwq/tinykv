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

## 2026-08-09

- Completed Project 3A (Raft membership change + leader transfer) and Project 3B (raftstore conf change / split / transfer leader).
- Project 3A in `raft/raft.go`:
  - `MsgTransferLeader` handling: check transferee membership, supersede pending transfers, help a lagging transferee catch up via `sendAppend`, then send `MsgTimeoutNow`; forward the request to the current leader when received by a follower.
  - `MsgTimeoutNow` handling: an eligible follower immediately campaigns (`MsgHup`); non-members and the leader ignore it.
  - `MsgAppendResponse` triggers `MsgTimeoutNow` once the transferee's match reaches the leader's last index.
  - Conf change gating with `PendingConfIndex` (one pending conf change at a time); proposals are rejected while a leader transfer is in progress.
  - `addNode`/`removeNode`: update `Prs`, re-run `maybeCommit` (so a pending command commits after quorum shrinks), and step down when the leader removes itself.
  - `MsgHup` on a raft with no peers (uninitialized peer awaiting snapshot) is a no-op, so such peers cannot inflate the term.
  - `sendHeartbeat` now attaches `min(pr.Match, committed)` as commit, so a brand-new follower sees commit 0 and the store worker recognizes the initial message and creates the peer.
- Project 3B in `kv/raftstore/peer_msg_handler.go` (+ small fixes in `peer.go`/`peer_storage.go`):
  - `proposeRaftCommand` special-cases `TransferLeader` (no log replication, answered immediately) and `ChangePeer` (proposed as `EntryConfChange` with the full request carried in `ConfChange.Context`).
  - `execChangePeer` applies the membership change to `Region`/`RegionEpoch.ConfVer`, persists `RegionLocalState` + apply state, calls `RawNode.ApplyConfChange`, refreshes the peer cache (critical when a peer is re-added on a different store), answers the proposal, and destroys the local peer on self-removal; duplicate commands are ignored via the proposal epoch.
  - `execSplit` applies the split command: increments `version`, splits the range, persists both regions, updates `storeMeta.regionRanges`/`regions`, registers/creates the new region's peer, and handles the race where the store worker already created the peer from an early heartbeat.
  - `HandleRaftReady` registers the applied snapshot region in `storeMeta` (needed for peers created uninitialized by conf change).
  - `SnapResponse` returns an immutable region copy so a client reading a later response cannot observe a mutated (post-split) region.
  - `ApplySnapshot` only cleans the previous range when the peer was already initialized; an uninitialized peer's placeholder `["","")` range would otherwise wipe the whole store before the snapshot is ingested.
  - Reject `AppendEntries` responses now carry the follower's last index as a hint so the leader jumps `next` instead of backing up one entry per round-trip under unreliable networks.
- Validation: `make project3a` and the complete `project3b` suite pass; a full combined `go test ./kv/test_raftstore -run '3B' -count=1` run passes. Raft 2A/2B/2C and `kv/raftstore` unit tests still pass; the full `2[BC]$` integration suite passes; Project 1 (`kv/server`) passes. `kv/transaction/mvcc` failures are pre-existing (Project 4, not yet implemented).

## 2026-08-09 (project 3C)

- Completed Project 3C (scheduler: region heartbeat collection + balance-region scheduler).
- `scheduler/server/cluster.go` — `processRegionHeartbeat`:
  - Rejects stale heartbeats by comparing `RegionEpoch` (first `version`, then `conf_ver`) against the recorded region; for a region new to the scheduler, the same check runs against every overlapping region (covers pre-split region ranges).
  - Skips redundant updates (`isRegionInfoChanged`: epoch bump, peer/pending-peer set change, leader change, approximate size change, or key range change), and otherwise refreshes the region tree plus the leader/region/pending/size status of every involved store (both the old and new peer sets).
  - Feeds every accepted heartbeat to the cluster `prepareChecker`.
- `scheduler/server/schedulers/balance_region.go` — `Schedule`:
  - Selects suitable source stores (largest region size) and target stores (smallest region size) via `StoreStateFilter{TransferLeader: true, MoveRegion: true}` (up + within `MaxStoreDownTime`, not busy, available).
  - For each source store (up to `balanceRegionRetryLimit` tries) picks the region to move: pending region first, then follower, then leader.
  - Picks the first target store not already holding a peer of the region; a move is only valuable when the region has full replicas and `sourceSize - targetSize > 2 * regionApproxSize` (prevents immediate move-back), then creates a `CreateMovePeerOperator`.
- Validation: `make project3c` passes; full `go test ./scheduler/...` (unit + integration) passes; gofmt/vet clean. `project3` is now fully complete (3A + 3B + 3C).
