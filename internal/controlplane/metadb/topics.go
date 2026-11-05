package metadb

import (
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/topics"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// These methods are the typed calls on the topics module. Each one opens the
// transaction and calls the matching function of the topics package.

// AssignState says what Assigned knows about a command.
type AssignState = topics.AssignState

// Assigned results.
const (
	OffsetOwnerUnknown   = topics.OffsetOwnerUnknown
	OffsetOwnedElsewhere = topics.OffsetOwnedElsewhere
	OffsetOwnedHere      = topics.OffsetOwnedHere
)

// Commit applies a CommitSegment that sits at log slot slot.
func (a *DB) Commit(slot int64, operation contract.CommitSegment) (runtime.ControlPlaneDone, error) {
	var d runtime.ControlPlaneDone
	err := a.keyValue.Update(func(transaction engine.Txn) { d = topics.Commit(transaction, slot, operation) })
	return d, err
}

// KnownCommit answers operation from the idempotency window when the key is
// already present. ok is false when the key is unknown (the caller must append).
func (a *DB) KnownCommit(operation contract.CommitSegment) (runtime.ControlPlaneDone, bool, error) {
	var d runtime.ControlPlaneDone
	var ok bool
	err := a.keyValue.View(func(transaction engine.Txn) { d, ok = topics.Known(transaction, operation) })
	return d, ok, err
}

// Lookup answers a Lookup from the stored runs.
func (a *DB) Lookup(operation contract.FindSegments) (runtime.ControlPlaneDone, error) {
	return a.Read(operation)
}

// Describe answers a DescribeTopics.
func (a *DB) Describe(operation contract.DescribeTopics) (runtime.ControlPlaneDone, error) {
	return a.Read(operation)
}

// CreateTopic creates name with at least partitions partitions (never
// shrinks). ErrInvalid for an empty name or partitions <= 0.
func (a *DB) CreateTopic(name string, partitions int32) runtime.ErrCode {
	return a.CreateTopicIf(name, partitions, false)
}

// CreateTopicIf is CreateTopic. With failIfExists, an existing topic gives
// ErrTopicExists and nothing changes.
func (a *DB) CreateTopicIf(name string, partitions int32, failIfExists bool) runtime.ErrCode {
	if name == "" || partitions <= 0 {
		return runtime.ErrInvalid
	}
	code := runtime.OK
	mustStore(a.keyValue.Update(func(transaction engine.Txn) {
		code = topics.Create(transaction, name, partitions, failIfExists)
	}))
	return code
}

// End is the next offset of topicPartition (0 if unknown).
func (a *DB) End(topicPartition contract.TopicPartition) contract.Offset {
	var end contract.Offset
	mustStore(a.keyValue.View(func(transaction engine.Txn) { end = topics.End(transaction, topicPartition) }))
	return end
}

// Trim forgets the idempotency records of commands at slots below slot and
// returns how many keys it dropped.
func (a *DB) Trim(slot int64) (int, error) {
	var dropped int
	err := a.keyValue.Update(func(transaction engine.Txn) { dropped = topics.Trim(transaction, slot) })
	return dropped, err
}

// Window is the number of segment keys currently remembered.
func (a *DB) Window() int {
	var n int
	_ = a.keyValue.View(func(transaction engine.Txn) { n = topics.Window(transaction) })
	return n
}

// Assigned reports the ranges operation was given if it is the first commit of
// its key at slot. An indexer uses it to write the run rows of a slot later.
func (a *DB) Assigned(slot int64, operation contract.CommitSegment) ([]contract.BatchRange, AssignState, error) {
	var rs []contract.BatchRange
	var st AssignState
	err := a.keyValue.View(func(transaction engine.Txn) { rs, st = topics.Assigned(transaction, slot, operation) })
	return rs, st, err
}

// OffsetFetch, DescribeGroup and ListGroups are the typed reads of the groups
// module.

// OffsetFetch answers an OffsetFetch.
func (a *DB) OffsetFetch(operation contract.OffsetFetch) (runtime.ControlPlaneDone, error) {
	return a.Read(operation)
}

// DescribeGroup answers a DescribeGroup.
func (a *DB) DescribeGroup(operation contract.DescribeGroup) (runtime.ControlPlaneDone, error) {
	return a.Read(operation)
}

// ListGroups answers a ListGroups.
func (a *DB) ListGroups(operation contract.ListGroups) (runtime.ControlPlaneDone, error) {
	return a.Read(operation)
}
