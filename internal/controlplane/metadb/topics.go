package metadb

import (
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/topics"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// These methods are the typed calls on the topics module. Each one opens the
// transaction and calls the matching function of the topics package.

// KnownCommit answers operation from the idempotency window when the key is
// already present. ok is false when the key is unknown (the caller must append).
func (a *DB) KnownCommit(operation contract.CommitSegment) (runtime.ControlPlaneDone, bool, error) {
	var d runtime.ControlPlaneDone
	var ok bool
	err := a.keyValue.View(func(transaction engine.Txn) { d, ok = topics.Known(transaction, operation) })
	return d, ok, err
}

// CreateTopic creates name with at least partitions partitions (never
// shrinks). ErrInvalid for an empty name or partitions <= 0. A store failure
// panics.
func (a *DB) CreateTopic(name string, partitions int32) runtime.ErrCode {
	if name == "" || partitions <= 0 {
		return runtime.ErrInvalid
	}
	code := runtime.OK
	mustStore(a.keyValue.Update(func(transaction engine.Txn) {
		code = topics.Create(transaction, name, partitions, false)
	}))
	return code
}

// Trim forgets the idempotency records of commands at slots below slot and
// returns how many keys it dropped.
func (a *DB) Trim(slot int64) (int, error) {
	var dropped int
	err := a.keyValue.Update(func(transaction engine.Txn) { dropped = topics.Trim(transaction, slot) })
	return dropped, err
}
