package storage

import (
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// The group commands, in the shape of a metadb schema row: each one applies
// one logged operation at its slot. The slot is not used: group state does not
// depend on where in the log a command sits. metadb lists them, with the three
// reads in read.go, in its group schema.

// Install applies a GroupInstall.
func Install(transaction engine.Txn, _ int64, operation contract.GroupInstall) runtime.ControlPlaneDone {
	return groupAnswer(installGroup(transaction, operation))
}

// Assign applies a GroupAssign.
func Assign(transaction engine.Txn, _ int64, operation contract.GroupAssign) runtime.ControlPlaneDone {
	return groupAnswer(assignGroup(transaction, operation))
}

// Remove applies a GroupRemove.
func Remove(transaction engine.Txn, _ int64, operation contract.GroupRemove) runtime.ControlPlaneDone {
	return groupAnswer(removeGroup(transaction, operation))
}

// CommitOffsets returns the command that applies an OffsetCommit. It asks
// partitions how many partitions a topic has, to check that a partition exists.
func CommitOffsets(partitions Partitions) func(engine.Txn, int64, contract.OffsetCommit) runtime.ControlPlaneDone {
	return func(transaction engine.Txn, _ int64, operation contract.OffsetCommit) runtime.ControlPlaneDone {
		return commitOffsets(transaction, operation, partitions)
	}
}
