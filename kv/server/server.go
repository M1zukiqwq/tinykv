package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.

// KvGet reads a value for the given key at the request's timestamp. If the key
// is locked by another transaction, the response carries a lock error.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	resp := new(kvrpcpb.GetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.IsLockedFor(req.Key, req.Version, resp) {
		return resp, nil
	}
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = value
	return resp, nil
}

// KvPrewrite is the first phase of two phase commit. It locks every key in the
// request and stores the value to be committed. If any key is locked by another
// transaction or has a conflicting write, nothing is written and the errors are
// returned.
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	resp := new(kvrpcpb.PrewriteResponse)
	keys := make([][]byte, 0, len(req.Mutations))
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, mutation := range req.Mutations {
		if keyErr := prewriteKey(txn, mutation, req.PrimaryLock, req.LockTtl); keyErr != nil {
			resp.Errors = append(resp.Errors, keyErr)
		}
	}
	if len(resp.Errors) > 0 {
		return resp, nil
	}
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	server.Latches.Validate(txn, keys)
	return resp, nil
}

// KvCommit is the second phase of two phase commit. It records a write at the
// commit timestamp for every key locked by the transaction and releases the
// locks. Repeated commits are idempotent; committing an already rolled back key
// fails.
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	resp := new(kvrpcpb.CommitResponse)
	keys := req.Keys
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			// A rollback marker means the transaction was aborted; it cannot be
			// committed any more.
			if write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{Abort: "transaction has been rolled back"}
				return resp, nil
			}
			// Already committed: a repeated commit request is a no-op.
			continue
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			// No lock: the prewrite was never received (or the key was already
			// rolled back), so there is nothing to commit.
			continue
		}
		if lock.Ts != req.StartVersion {
			// Locked by a different transaction.
			resp.Error = &kvrpcpb.KeyError{Retryable: "key is locked by another transaction"}
			return resp, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: lock.Kind})
		txn.DeleteLock(key)
	}
	if resp.Error != nil {
		return resp, nil
	}
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	server.Latches.Validate(txn, keys)
	return resp, nil
}

// prewriteKey stages a single mutation for the prewrite phase. It returns a
// KeyError if the key is locked by another transaction or conflicts with a newer
// committed write; otherwise it adds the lock and value writes to the txn.
func prewriteKey(txn *mvcc.MvccTxn, mutation *kvrpcpb.Mutation, primary []byte, lockTtl uint64) *kvrpcpb.KeyError {
	lock, err := txn.GetLock(mutation.Key)
	if err != nil {
		return &kvrpcpb.KeyError{Retryable: err.Error()}
	}
	if lock != nil {
		return &kvrpcpb.KeyError{Locked: lock.Info(mutation.Key)}
	}
	// A newer write committed after this transaction started means the value
	// this transaction would write is already obsolete.
	write, commitTs, err := txn.MostRecentWrite(mutation.Key)
	if err != nil {
		return &kvrpcpb.KeyError{Retryable: err.Error()}
	}
	if write != nil && commitTs > txn.StartTS {
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    txn.StartTS,
				ConflictTs: write.StartTS,
				Key:        mutation.Key,
				Primary:    primary,
			},
		}
	}
	var kind mvcc.WriteKind
	switch mutation.Op {
	case kvrpcpb.Op_Put:
		kind = mvcc.WriteKindPut
	case kvrpcpb.Op_Del:
		kind = mvcc.WriteKindDelete
	default:
		return &kvrpcpb.KeyError{Abort: "unsupported mutation operation"}
	}
	txn.PutLock(mutation.Key, &mvcc.Lock{
		Primary: primary,
		Ts:      txn.StartTS,
		Ttl:     lockTtl,
		Kind:    kind,
	})
	switch mutation.Op {
	case kvrpcpb.Op_Put:
		txn.PutValue(mutation.Key, mutation.Value)
	case kvrpcpb.Op_Del:
		txn.DeleteValue(mutation.Key)
	}
	return nil
}

// KvScan reads multiple values starting from the given key, at the request's
// timestamp. It returns at most `limit` key/value pairs.
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	resp := new(kvrpcpb.ScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	for uint32(len(resp.Pairs)) < req.Limit {
		key, value, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Value: value})
	}
	return resp, nil
}

// KvCheckTxnStatus reports the status of the transaction identified by the
// primary key and lock ts. If the lock has expired, it is rolled back.
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	resp := new(kvrpcpb.CheckTxnStatusResponse)
	server.Latches.WaitForLatches([][]byte{req.PrimaryKey})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryKey})

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock != nil {
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			// The lock has expired: roll back the primary key so the client can
			// resolve the rest of the transaction.
			txn.DeleteLock(req.PrimaryKey)
			txn.DeleteValue(req.PrimaryKey)
			txn.PutWrite(req.PrimaryKey, lock.Ts, &mvcc.Write{StartTS: lock.Ts, Kind: mvcc.WriteKindRollback})
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			resp.LockTtl = 0
		} else {
			resp.Action = kvrpcpb.Action_NoAction
			resp.LockTtl = lock.Ttl
		}
	} else {
		write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
		if err != nil {
			return nil, err
		}
		if write != nil {
			// The transaction was already committed or rolled back.
			resp.Action = kvrpcpb.Action_NoAction
			if write.Kind != mvcc.WriteKindRollback {
				resp.CommitVersion = commitTs
			}
		} else {
			// No lock and no write: the transaction never reached this key.
			// Leave a rollback marker so a later commit cannot succeed.
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{StartTS: req.LockTs, Kind: mvcc.WriteKindRollback})
			resp.Action = kvrpcpb.Action_LockNotExistRollback
		}
	}
	if len(txn.Writes()) > 0 {
		if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		server.Latches.Validate(txn, [][]byte{req.PrimaryKey})
	}
	return resp, nil
}

// KvBatchRollback rolls back a transaction: for each key it removes the lock
// (if it belongs to the transaction), deletes any prewritten value, and leaves
// a rollback marker so the key can never be committed later.
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	resp := new(kvrpcpb.BatchRollbackResponse)
	keys := req.Keys
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, key := range keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				// The key was already committed; it cannot be rolled back.
				resp.Error = &kvrpcpb.KeyError{Abort: "transaction is already committed"}
				return resp, nil
			}
			// Already rolled back: a repeated rollback is a no-op.
			continue
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == req.StartVersion {
			// The lock belongs to this transaction: remove it and the
			// prewritten value.
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}
		// Leave a rollback marker. Keys locked by another transaction are left
		// untouched (only the marker is recorded).
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
	}
	if resp.Error != nil {
		return resp, nil
	}
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	server.Latches.Validate(txn, keys)
	return resp, nil
}

// KvResolveLock resolves all locks of the transaction identified by the request:
// if a commit timestamp is given the locks are committed, otherwise they are
// rolled back.
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	resp := new(kvrpcpb.ResolveLockResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	locks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		return nil, err
	}
	keys := make([][]byte, 0, len(locks))
	for _, kl := range locks {
		keys = append(keys, kl.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	for _, kl := range locks {
		if req.CommitVersion > 0 {
			txn.PutWrite(kl.Key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: kl.Lock.Kind})
			txn.DeleteLock(kl.Key)
		} else {
			txn.DeleteLock(kl.Key)
			txn.DeleteValue(kl.Key)
			txn.PutWrite(kl.Key, req.StartVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: mvcc.WriteKindRollback})
		}
	}
	if len(txn.Writes()) > 0 {
		if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		server.Latches.Validate(txn, keys)
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
