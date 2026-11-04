package storage

import (
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/kv"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/module"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Module is the groups context in the replicated state: the four group writes
// are logged and the three group reads are not. It implements module.Module.
type Module struct{ partitions Partitions }

var _ module.Module = Module{}

// NewModule returns the module. An offset commit asks partitions how many
// partitions its topic has.
func NewModule(partitions Partitions) Module { return Module{partitions: partitions} }

// Mode reports how the module treats operation.
func (Module) Mode(operation runtime.ControlPlaneOperation) module.Mode {
	switch operation.(type) {
	case contract.GroupInstall, contract.GroupAssign, contract.GroupRemove, contract.OffsetCommit:
		return module.Logged
	case contract.OffsetFetch, contract.DescribeGroup, contract.ListGroups:
		return module.ReadOnly
	}
	return module.NotMine
}

// Write applies a group write. The slot is not used: group state does not
// depend on where in the log a command sits.
func (m Module) Write(transaction kv.Transaction, _ int64, operation runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
	return write(transaction, operation, m.partitions)
}

// Read answers a group read.
func (Module) Read(transaction kv.Transaction, operation runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
	switch x := operation.(type) {
	case contract.OffsetFetch:
		return offsetFetch(transaction, x)
	case contract.DescribeGroup:
		return describeGroup(transaction, x)
	case contract.ListGroups:
		return listGroups(transaction, x)
	}
	return runtime.ControlPlaneDone{Err: runtime.ErrInvalid}
}
