package metadb

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/topics"
)

// StateHash is a digest of the materialised state (topics, ends, runs), not of
// the idempotency window: two metadata stores that applied the same commands in the
// same order agree, whatever they trimmed. The model has no state hash.
func (a *Store) StateHash() uint64 {
	h := fnv.New64a()
	_ = a.keyValue.View(func(transaction engine.Txn) {
		transaction.Ascend([]byte{0}, nil, func(k, v []byte) bool {
			if !topics.InWindow(k) {
				if _, err := h.Write(binary.AppendUvarint(nil, uint64(len(k)))); err != nil {
					return false
				}
				if _, err := h.Write(k); err != nil {
					return false
				}
				if _, err := h.Write(v); err != nil {
					return false
				}
			}
			return true
		})
	})
	return h.Sum64()
}
