package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
// The scanner walks the write CF: encoded keys are ordered by user key ascending
// and, within a user key, by timestamp descending, so a seek to (startKey, MaxTS)
// lands on the first (user key, newest write) pair at or after startKey.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(startKey, TsMax))
	return &Scanner{txn: txn, iter: iter}
}

func (scan *Scanner) Close() {
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner, reading each key's value
// as of the transaction's start timestamp. If the scanner is exhausted, then it
// will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	for scan.iter.Valid() {
		userKey := DecodeUserKey(scan.iter.Item().Key())
		// Walk the versions of userKey from the most recent to the oldest and
		// return the newest write committed at or before the start timestamp.
		// Versions committed later, rollback markers and values hidden by a
		// newer delete are all skipped.
		deleted := false
		for scan.iter.Valid() {
			item := scan.iter.Item()
			if !bytes.Equal(DecodeUserKey(item.Key()), userKey) {
				break
			}
			commitTs := decodeTimestamp(item.Key())
			value, err := item.ValueCopy(nil)
			if err != nil {
				return nil, nil, err
			}
			write, err := ParseWrite(value)
			if err != nil {
				return nil, nil, err
			}
			scan.iter.Next()

			if write.Kind == WriteKindRollback || commitTs > scan.txn.StartTS {
				continue
			}
			if write.Kind == WriteKindDelete {
				deleted = true
				continue
			}
			if deleted {
				continue
			}
			val, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
			if err != nil {
				return nil, nil, err
			}
			return userKey, val, nil
		}
	}
	return nil, nil, nil
}
