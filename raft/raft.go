// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomized election interval, in (electionTimeout, 2*electionTimeout)
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	raftLog := newLog(c.Storage)
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	peers := c.peers
	if len(peers) == 0 {
		peers = confState.Nodes
	}

	prs := make(map[uint64]*Progress, len(peers))
	for _, peer := range peers {
		prs[peer] = &Progress{
			Match: 0,
			Next:  raftLog.LastIndex() + 1,
		}
	}

	if hardState.Commit > raftLog.committed {
		raftLog.committed = hardState.Commit
	}
	if c.Applied > raftLog.applied {
		raftLog.applied = c.Applied
	}

	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          raftLog,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	r.resetRandomizedElectionTimeout()
	return r
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + 1
	if r.electionTimeout > 1 {
		r.randomizedElectionTimeout += rand.Intn(r.electionTimeout - 1)
	}
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) appendEntry(entries ...pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = lastIndex + 1 + uint64(i)
	}
	r.RaftLog.entries = append(r.RaftLog.entries, entries...)
	if pr := r.Prs[r.id]; pr != nil {
		pr.Match = r.RaftLog.LastIndex()
		pr.Next = pr.Match + 1
	}
}

func (r *Raft) maybeCommit() bool {
	for index := r.RaftLog.LastIndex(); index > r.RaftLog.committed; index-- {
		term, err := r.RaftLog.Term(index)
		if err != nil || term != r.Term {
			continue
		}

		matches := 0
		for _, pr := range r.Prs {
			if pr.Match >= index {
				matches++
			}
		}
		if matches >= r.quorum() {
			r.RaftLog.committed = index
			return true
		}
	}
	return false
}

func (r *Raft) isLogUpToDate(index uint64, term uint64) bool {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		return false
	}
	return term > lastTerm || term == lastTerm && index >= lastIndex
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr, ok := r.Prs[to]
	if !ok {
		return false
	}
	r.RaftLog.maybeCompact()

	prevIndex := pr.Next - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		var snapshot pb.Snapshot
		var snapshotErr error
		if r.RaftLog.pendingSnapshot != nil {
			snapshot = *r.RaftLog.pendingSnapshot
		} else {
			snapshot, snapshotErr = r.RaftLog.storage.Snapshot()
		}
		if snapshotErr != nil || IsEmptySnap(&snapshot) {
			return false
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot,
			From:     r.id,
			To:       to,
			Term:     r.Term,
			Snapshot: &snapshot,
		})
		return true
	}

	entries := make([]*pb.Entry, 0)
	if pr.Next <= r.RaftLog.LastIndex() {
		offset := r.RaftLog.entries[0].Index
		start := pr.Next - offset
		if start < 1 {
			start = 1
		}
		for _, entry := range r.RaftLog.entries[start:] {
			ent := entry
			entries = append(entries, &ent)
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: prevTerm,
		Index:   prevIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	if term > r.Term {
		r.Vote = None
	}
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.Lead = None
	r.Vote = r.id
	r.votes = map[uint64]bool{r.id: true}
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0

	next := r.RaftLog.LastIndex() + 1
	for id := range r.Prs {
		r.Prs[id].Match = 0
		r.Prs[id].Next = next
	}

	// NOTE: Leader should propose a noop entry on its term.
	r.appendEntry(pb.Entry{})
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State == StateLeader {
			return nil
		}
		r.becomeCandidate()
		if len(r.Prs) == 0 || r.votes[r.id] && r.quorum() == 1 {
			r.becomeLeader()
			return nil
		}

		lastIndex := r.RaftLog.LastIndex()
		lastTerm, err := r.RaftLog.Term(lastIndex)
		if err != nil {
			return err
		}
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      id,
				Term:    r.Term,
				LogTerm: lastTerm,
				Index:   lastIndex,
			})
		}

	case pb.MessageType_MsgRequestVote:
		reject := true
		if m.Term < r.Term {
			reject = true
		} else if (r.Vote == None || r.Vote == m.From) && r.isLogUpToDate(m.Index, m.LogTerm) {
			reject = false
			r.Vote = m.From
			r.electionElapsed = 0
			r.resetRandomizedElectionTimeout()
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  reject,
		})

	case pb.MessageType_MsgRequestVoteResponse:
		if r.State != StateCandidate || m.Term != r.Term {
			return nil
		}
		r.votes[m.From] = !m.Reject

		granted := 0
		rejected := 0
		for _, vote := range r.votes {
			if vote {
				granted++
			} else {
				rejected++
			}
		}
		if granted >= r.quorum() {
			r.becomeLeader()
		} else if rejected >= r.quorum() {
			r.becomeFollower(r.Term, None)
		}

	case pb.MessageType_MsgBeat:
		if r.State != StateLeader {
			return nil
		}
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendHeartbeat(id)
		}

	case pb.MessageType_MsgPropose:
		if r.State != StateLeader {
			return ErrProposalDropped
		}
		if len(m.Entries) == 0 {
			return nil
		}
		entries := make([]pb.Entry, 0, len(m.Entries))
		for _, entry := range m.Entries {
			entries = append(entries, *entry)
		}
		r.appendEntry(entries...)
		if len(r.Prs) == 1 {
			r.RaftLog.committed = r.RaftLog.LastIndex()
			return nil
		}
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	case pb.MessageType_MsgSnapshot:
		if m.Snapshot == nil || m.Snapshot.Metadata == nil || m.Term < r.Term {
			return nil
		}
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)

	case pb.MessageType_MsgAppendResponse:
		if r.State != StateLeader || m.Term != r.Term {
			return nil
		}
		pr := r.Prs[m.From]
		if pr == nil {
			return nil
		}
		if m.Reject {
			if pr.Next > 1 {
				pr.Next--
			}
			r.sendAppend(m.From)
			return nil
		}
		if m.Index > pr.Match {
			pr.Match = m.Index
			pr.Next = pr.Match + 1
		}
		if r.maybeCommit() {
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendAppend(id)
			}
		}

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	case pb.MessageType_MsgHeartbeatResponse:
		if r.State == StateLeader && m.Term == r.Term {
			r.sendAppend(m.From)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			Reject:  true,
		})
		return
	}

	r.becomeFollower(m.Term, m.From)
	prevTerm, err := r.RaftLog.Term(m.Index)
	if err != nil || prevTerm != m.LogTerm {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   m.Index,
			Reject:  true,
		})
		return
	}

	lastNewIndex := m.Index
	if len(m.Entries) > 0 {
		lastNewIndex = m.Entries[len(m.Entries)-1].Index
	}
	for _, entry := range m.Entries {
		localTerm, err := r.RaftLog.Term(entry.Index)
		if err == nil && localTerm == entry.Term {
			continue
		}

		offset := r.RaftLog.entries[0].Index
		// The entry before the in-memory log is the snapshot boundary. A
		// leader must match that boundary in m.Index; accepting an entry at or
		// before it would require slicing before the dummy entry and can only
		// represent a stale or malformed AppendEntries request.
		if entry.Index <= offset || entry.Index < m.Entries[0].Index {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Index:   m.Index,
				Reject:  true,
			})
			return
		}
		if entry.Index <= r.RaftLog.LastIndex() {
			r.RaftLog.entries = r.RaftLog.entries[:entry.Index-offset]
		}
		if r.RaftLog.stabled >= entry.Index {
			r.RaftLog.stabled = entry.Index - 1
		}
		for _, newEntry := range m.Entries[entry.Index-m.Entries[0].Index:] {
			r.RaftLog.entries = append(r.RaftLog.entries, *newEntry)
		}
		break
	}

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, lastNewIndex)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}

	r.becomeFollower(m.Term, m.From)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	if m.Snapshot == nil || m.Snapshot.Metadata == nil {
		return
	}

	snapshot := m.Snapshot
	index := snapshot.Metadata.Index
	term := snapshot.Metadata.Term
	if index <= r.RaftLog.committed {
		if m.From != None {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Index:   r.RaftLog.committed,
			})
		}
		return
	}

	// If the local log already contains the snapshot point, the state machine
	// can catch up by applying its existing entries; no state transfer is
	// needed.
	if localTerm, err := r.RaftLog.Term(index); err == nil && localTerm == term {
		r.RaftLog.committed = index
		if m.From != None {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
			})
		}
		return
	}

	// Replace the in-memory suffix with the snapshot boundary. The snapshot is
	// intentionally kept pending until Ready has been persisted and applied by
	// the upper layer.
	snapshotCopy := *snapshot
	r.RaftLog.pendingSnapshot = &snapshotCopy
	r.RaftLog.entries = []pb.Entry{{Index: index, Term: term}}
	r.RaftLog.committed = index
	r.RaftLog.stabled = index

	r.Prs = make(map[uint64]*Progress)
	if snapshot.Metadata.ConfState != nil {
		for _, id := range snapshot.Metadata.ConfState.Nodes {
			progress := &Progress{Next: index + 1}
			if id == r.id {
				progress.Match = index
			}
			r.Prs[id] = progress
		}
	}
	r.votes = make(map[uint64]bool)

	if m.From != None {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   index,
		})
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
