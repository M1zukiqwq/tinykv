package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	if !d.RaftGroup.HasReady() {
		return
	}
	rd := d.RaftGroup.Ready()
	// 1) Persist HardState / Entries (and Snapshot when 2C is implemented).
	applySnapResult, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		panic(err)
	}
	if applySnapResult != nil && applySnapResult.Region != nil {
		// The snapshot may carry newer region metadata. The durable region
		// state was written by PeerStorage; update the live peer before it
		// handles any subsequent messages or proposals.
		d.SetRegion(applySnapResult.Region)
		// Register the applied region in the store metadata. A peer created
		// from a conf change (or split) is not yet inserted into
		// regionRanges; that insert is deferred until its snapshot is applied.
		// Snapshot ranges never overlap existing regions (checkSnapshot
		// rejects them), so an idempotent insert is sufficient.
		meta := d.ctx.storeMeta
		meta.Lock()
		meta.regions[d.regionId] = applySnapResult.Region
		meta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapResult.Region})
		meta.Unlock()
	}
	// 2) Send raft messages only after the corresponding state is durable.
	d.Send(d.ctx.trans, rd.Messages)
	// 3) Apply committed entries to the KV state machine and answer proposals.
	for _, entry := range rd.CommittedEntries {
		d.processCommittedEntry(entry)
		if d.stopped {
			return
		}
	}
	// 4) Tell Raft we have consumed this Ready.
	d.RaftGroup.Advance(rd)
}

// processCommittedEntry applies one committed raft log entry.
func (d *peerMsgHandler) processCommittedEntry(entry eraftpb.Entry) {
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		d.execChangePeer(entry)
		return
	}
	// Empty data is the leader's noop entry after election.
	if len(entry.Data) == 0 {
		d.persistAppliedIndex(entry.Index)
		return
	}

	req := &raft_cmdpb.RaftCmdRequest{}
	if err := req.Unmarshal(entry.Data); err != nil {
		panic(err)
	}

	if req.AdminRequest != nil {
		d.execAdminRequest(req, entry)
		return
	}
	d.execNormalRequest(req, entry)
}

func (d *peerMsgHandler) persistAppliedIndex(index uint64) {
	d.peerStorage.applyState.AppliedIndex = index
	kvWB := new(engine_util.WriteBatch)
	if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
		panic(err)
	}
	if err := kvWB.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
		panic(err)
	}
}

func (d *peerMsgHandler) execAdminRequest(req *raft_cmdpb.RaftCmdRequest, entry eraftpb.Entry) {
	admin := req.AdminRequest
	kvWB := new(engine_util.WriteBatch)
	resp := newCmdResp()
	BindRespTerm(resp, d.Term())

	switch admin.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		// Update truncated metadata; actual log deletion is async via raftlog-gc worker.
		compact := admin.GetCompactLog()
		if compact.GetCompactIndex() >= d.peerStorage.applyState.TruncatedState.Index {
			d.peerStorage.applyState.TruncatedState.Index = compact.GetCompactIndex()
			d.peerStorage.applyState.TruncatedState.Term = compact.GetCompactTerm()
			d.ScheduleCompactLog(compact.GetCompactIndex())
		}
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
			CompactLog: &raft_cmdpb.CompactLogResponse{},
		}
	case raft_cmdpb.AdminCmdType_Split:
		d.execSplit(req, entry)
		return
	default:
		// Unknown admin commands (e.g. TransferLeader, which is not replicated
		// through the log) are ignored here.
		d.persistAppliedIndex(entry.Index)
		return
	}

	d.peerStorage.applyState.AppliedIndex = entry.Index
	if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
		panic(err)
	}
	if err := kvWB.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
		panic(err)
	}
	d.notifyProposal(entry, resp, nil)
}

func (d *peerMsgHandler) execNormalRequest(req *raft_cmdpb.RaftCmdRequest, entry eraftpb.Entry) {
	kvWB := new(engine_util.WriteBatch)
	resp := newCmdResp()
	BindRespTerm(resp, d.Term())
	needSnap := false

	// Re-check epoch at apply time so a stale command that lost leadership /
	// region epoch races returns a proper region error instead of silently applying.
	if err := util.CheckRegionEpoch(req, d.Region(), true); err != nil {
		BindRespError(resp, err)
		d.peerStorage.applyState.AppliedIndex = entry.Index
		if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
			panic(err)
		}
		if err := kvWB.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
			panic(err)
		}
		d.notifyProposal(entry, resp, nil)
		return
	}

	for _, r := range req.Requests {
		switch r.CmdType {
		case raft_cmdpb.CmdType_Get:
			key := r.Get.GetKey()
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				BindRespError(resp, err)
				break
			}
			// Flush pending writes so this Get observes earlier puts in the same request.
			if kvWB.Len() > 0 {
				if err := kvWB.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
					panic(err)
				}
				kvWB = new(engine_util.WriteBatch)
			}
			val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, r.Get.GetCf(), key)
			if err == badger.ErrKeyNotFound {
				val = nil
			} else if err != nil {
				panic(err)
			}
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get:     &raft_cmdpb.GetResponse{Value: val},
			})
		case raft_cmdpb.CmdType_Put:
			put := r.GetPut()
			if err := util.CheckKeyInRegion(put.GetKey(), d.Region()); err != nil {
				BindRespError(resp, err)
				break
			}
			kvWB.SetCF(put.GetCf(), put.GetKey(), put.GetValue())
			d.SizeDiffHint += uint64(len(put.GetKey()) + len(put.GetValue()))
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			})
		case raft_cmdpb.CmdType_Delete:
			del := r.GetDelete()
			if err := util.CheckKeyInRegion(del.GetKey(), d.Region()); err != nil {
				BindRespError(resp, err)
				break
			}
			kvWB.DeleteCF(del.GetCf(), del.GetKey())
			d.SizeDiffHint += uint64(len(del.GetKey()))
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			})
		case raft_cmdpb.CmdType_Snap:
			needSnap = true
			// Return an immutable copy of the region. The live region object
			// is mutated in place by later admin commands (e.g. Split), and a
			// client reading this response afterwards must not observe the
			// mutated range.
			snapRegion := new(metapb.Region)
			if err := util.CloneMsg(d.Region(), snapRegion); err != nil {
				panic(err)
			}
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: snapRegion},
			})
		}
		if resp.Header != nil && resp.Header.Error != nil {
			break
		}
	}

	d.peerStorage.applyState.AppliedIndex = entry.Index
	if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
		panic(err)
	}
	if err := kvWB.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
		panic(err)
	}

	var txn *badger.Txn
	// Open the read txn only after all writes for this entry are durable.
	if needSnap && (resp.Header == nil || resp.Header.Error == nil) {
		txn = d.peerStorage.Engines.Kv.NewTransaction(false)
	}
	d.notifyProposal(entry, resp, txn)
}

// notifyProposal matches a committed entry to a pending client proposal and
// completes its callback. Stale proposals (overwritten after leader change)
// receive ErrStaleCommand so the client can retry.
func (d *peerMsgHandler) notifyProposal(entry eraftpb.Entry, resp *raft_cmdpb.RaftCmdResponse, txn *badger.Txn) {
	for len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.index < entry.Index {
			NotifyStaleReq(d.Term(), p.cb)
			d.proposals = d.proposals[1:]
			continue
		}
		if p.index > entry.Index {
			// Entry was proposed elsewhere (e.g. follower apply); nothing to answer.
			return
		}
		// p.index == entry.Index
		if p.term != entry.Term {
			NotifyStaleReq(d.Term(), p.cb)
		} else {
			if txn != nil && p.cb != nil {
				p.cb.Txn = txn
			}
			p.cb.Done(resp)
		}
		d.proposals = d.proposals[1:]
		return
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	if msg.AdminRequest != nil && msg.AdminRequest.CmdType == raft_cmdpb.AdminCmdType_TransferLeader {
		// TransferLeader is an action that does not need to be replicated to
		// other peers; ask the current leader directly to transfer leadership.
		d.proposeTransferLeader(msg.AdminRequest, cb)
		return
	}
	// Serialize the whole command; the Raft log only carries raw bytes.
	data, err := msg.Marshal()
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Record the callback tagged with the index this entry will occupy and the
	// current term, so HandleRaftReady can find and answer it once applied.
	// nextProposalIndex() == RaftLog.LastIndex()+1, computed before Propose appends.
	p := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}
	d.proposals = append(d.proposals, p)
	// Membership changes are proposed as EntryConfChange entries. Only
	// ChangeType and the peer id are replicated; the full request is attached
	// in ConfChange.Context so the apply path can resolve the peer's store id
	// and the request epoch (for duplicate detection).
	if admin := msg.AdminRequest; admin != nil && admin.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
		changePeer := admin.ChangePeer
		cc := eraftpb.ConfChange{
			ChangeType: changePeer.GetChangeType(),
			NodeId:     changePeer.GetPeer().GetId(),
			Context:    data,
		}
		if err := d.RaftGroup.ProposeConfChange(cc); err != nil {
			// Drop the dangling proposal so it is not answered later incorrectly.
			d.proposals = d.proposals[:len(d.proposals)-1]
			cb.Done(ErrResp(err))
			return
		}
		return
	}
	if err := d.RaftGroup.Propose(data); err != nil {
		// Drop the dangling proposal so it is not answered later incorrectly.
		d.proposals = d.proposals[:len(d.proposals)-1]
		cb.Done(ErrResp(err))
		return
	}
}

// proposeTransferLeader asks the current leader to transfer its leadership to
// the target peer and answers the client immediately (the action itself is not
// replicated through the Raft log).
func (d *peerMsgHandler) proposeTransferLeader(admin *raft_cmdpb.AdminRequest, cb *message.Callback) {
	transferee := admin.GetTransferLeader().GetPeer()
	if transferee == nil {
		cb.Done(ErrResp(errors.Errorf("%s transfer leader request misses the target peer", d.Tag)))
		return
	}
	d.RaftGroup.TransferLeader(transferee.GetId())
	resp := newCmdResp()
	BindRespTerm(resp, d.Term())
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
		TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
	}
	cb.Done(resp)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

// execChangePeer applies a committed conf change to the region metadata.
// The original ChangePeer request is recovered from the ConfChange context.
func (d *peerMsgHandler) execChangePeer(entry eraftpb.Entry) {
	var cc eraftpb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		panic(err)
	}
	admin := new(raft_cmdpb.AdminRequest)
	req := new(raft_cmdpb.RaftCmdRequest)
	if len(cc.Context) > 0 {
		if err := req.Unmarshal(cc.Context); err != nil {
			panic(err)
		}
		admin = req.AdminRequest
	}
	changePeer := admin.GetChangePeer()
	peer := changePeer.GetPeer()
	region := d.Region()

	// A ChangePeer command is uniquely identified by its peer and the epoch at
	// the time it was proposed. Ignore duplicates, which may happen when the
	// command is proposed multiple times before it is applied.
	reqEpoch := req.GetHeader().GetRegionEpoch()
	if reqEpoch != nil && util.IsEpochStale(reqEpoch, region.GetRegionEpoch()) {
		log.Warnf("%s stale conf change, region_epoch %s, req_epoch %s, skip", d.Tag, region.GetRegionEpoch(), reqEpoch)
		d.persistAppliedIndex(entry.Index)
		return
	}

	resp := newCmdResp()
	BindRespTerm(resp, d.Term())

	kvWB := new(engine_util.WriteBatch)
	switch changePeer.GetChangeType() {
	case eraftpb.ConfChangeType_AddNode:
		if util.FindPeer(region, peer.GetStoreId()) != nil {
			// The peer already exists in the region; this is a duplicated
			// command, ignore it.
			log.Warnf("%s add duplicated peer %v to region %v, skip", d.Tag, peer, region)
		} else {
			region.Peers = append(region.Peers, peer)
			region.RegionEpoch.ConfVer++
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if util.FindPeer(region, peer.GetStoreId()) == nil {
			log.Warnf("%s remove missing peer %v from region %v, skip", d.Tag, peer, region)
		} else {
			util.RemovePeer(region, peer.GetStoreId())
			region.RegionEpoch.ConfVer++
		}
	}

	// Persist the new region state before notifying Raft of the membership change.
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	d.peerStorage.applyState.AppliedIndex = entry.Index
	if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
		panic(err)
	}
	if err := kvWB.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
		panic(err)
	}

	// Update the region in memory and the store metadata.
	d.SetRegion(region)
	metaRegion := d.ctx.storeMeta
	metaRegion.Lock()
	metaRegion.regions[d.regionId] = region
	metaRegion.Unlock()

	// Rebuild the peer cache from the new membership. A stale cache entry
	// (e.g. a peer re-added on a different store) would otherwise make the
	// leader keep routing messages to the old store forever.
	d.peerCache = make(map[uint64]*metapb.Peer)
	for _, p := range region.GetPeers() {
		d.insertPeerCache(p)
	}

	// Tell Raft to apply the membership change, then answer the proposal.
	d.RaftGroup.ApplyConfChange(cc)
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: region},
	}
	d.notifyProposal(entry, resp, nil)

	// Remove the peer: after the conf change is applied, the removed node
	// should destroy itself. The new peer (if any) is created by the store
	// worker upon receiving the leader's heartbeat.
	if changePeer.GetChangeType() == eraftpb.ConfChangeType_RemoveNode && peer.GetStoreId() == d.storeID() {
		if !d.stopped {
			d.destroyPeer()
		}
	}
}

// execSplit applies a committed split command, dividing the current region
// into two at the split key.
func (d *peerMsgHandler) execSplit(req *raft_cmdpb.RaftCmdRequest, entry eraftpb.Entry) {
	split := req.AdminRequest.GetSplit()
	splitKey := split.GetSplitKey()
	region := d.Region()

	// The split key must be strictly inside the region range (start_key, end_key).
	if bytes.Compare(splitKey, region.GetStartKey()) <= 0 || engine_util.ExceedEndKey(splitKey, region.GetEndKey()) {
		err := &util.ErrKeyNotInRegion{Key: splitKey, Region: region}
		resp := ErrRespWithTerm(err, d.Term())
		d.persistAppliedIndex(entry.Index)
		d.notifyProposal(entry, resp, nil)
		return
	}

	regionEpoch := region.GetRegionEpoch()
	// The splitting region (which inherits the original id) has its version
	// incremented; the newly created region starts from the incremented version.
	regionEpoch.Version++
	newRegion := &metapb.Region{
		Id:          split.GetNewRegionId(),
		StartKey:    util.SafeCopy(splitKey),
		EndKey:      util.SafeCopy(region.EndKey),
		RegionEpoch: &metapb.RegionEpoch{ConfVer: regionEpoch.ConfVer, Version: regionEpoch.Version},
	}
	region.EndKey = util.SafeCopy(splitKey)
	region.RegionEpoch = regionEpoch

	// Assign the new region's peers by pairing the pre-allocated peer ids
	// with the store ids of the current region's peers.
	newPeers := split.GetNewPeerIds()
	for i, peer := range region.Peers {
		if uint64(i) >= uint64(len(newPeers)) {
			break
		}
		newRegion.Peers = append(newRegion.Peers, &metapb.Peer{
			Id:      newPeers[i],
			StoreId: peer.GetStoreId(),
		})
	}

	kvWB := new(engine_util.WriteBatch)
	// Persist the region state of the left region (this region) and the new
	// right region.
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	d.peerStorage.applyState.AppliedIndex = entry.Index
	if err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
		panic(err)
	}
	if err := kvWB.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
		panic(err)
	}

	// Update this peer's in-memory region and the store metadata.
	d.SetRegion(region)
	metaRegion := d.ctx.storeMeta
	metaRegion.Lock()
	if metaRegion.regionRanges.Delete(&regionItem{region: region}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	metaRegion.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	metaRegion.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	metaRegion.regions[d.regionId] = region
	metaRegion.regions[newRegion.Id] = newRegion
	// The new region's peer may already exist: the store worker creates it
	// when a heartbeat from the new region's leader arrives before this split
	// is applied locally (see maybeCreatePeer). Refresh its in-memory region
	// atomically with the store metadata so a pending snapshot is not rejected
	// as stale.
	existingPeer := d.ctx.router.get(newRegion.Id)
	if existingPeer != nil {
		existingPeer.peer.SetRegion(newRegion)
	}
	metaRegion.Unlock()

	// Create the peer of the newly created region and register it to the
	// router, so the right part can serve requests right away. If the peer was
	// already created above, it will be initialized by the leader's snapshot.
	if existingPeer == nil && util.FindPeer(newRegion, d.storeID()) != nil {
		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			panic(err)
		}
		d.ctx.router.register(newPeer)
		newPeer.MaybeCampaign(d.IsLeader())
		d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
	}

	resp := newCmdResp()
	BindRespTerm(resp, d.Term())
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{region, newRegion}},
	}
	d.notifyProposal(entry, resp, nil)

	// Create the peer of the newly created region and register it to the
	// router, so the right part can serve requests right away. The peer may
	// already exist here if a heartbeat (or snapshot) from the new region's
	// leader arrived before this split command was applied locally — in that
	// case the store worker has already created it (see maybeCreatePeer) and
	// it will be initialized by the leader's snapshot.
	if util.FindPeer(newRegion, d.storeID()) != nil && d.ctx.router.get(newRegion.Id) == nil {
		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			panic(err)
		}
		d.ctx.router.register(newPeer)
		newPeer.MaybeCampaign(d.IsLeader())
		d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
	}
}
