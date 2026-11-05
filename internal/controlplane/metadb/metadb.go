package metadb

import (
	"fmt"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/groups/storage"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/module"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/topics"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// DB is the control-plane replicated state over an engine.Engine. It is the kernel:
// it holds the modules (topics, groups) and sends each operation to the one that
// owns it. Writes (commands) must be applied by one goroutine in log order;
// reads may run concurrently.
type DB struct {
	keyValue engine.Engine
	modules  []module.Module
}

// New returns a metadb over the engine (empty, or restored from a checkpoint).
func New(keyValue engine.Engine) *DB {
	return &DB{keyValue: keyValue, modules: []module.Module{topics.Module{}, storage.NewModule(topics.PartitionCount)}}
}

// Close closes the store.
func (a *DB) Close() error { return a.keyValue.Close() }

// mustStore panics when a store call failed. This is on purpose: the
// metadb stops (fail-stop) instead of going on with bad state.
func mustStore(err error) {
	if err != nil {
		panic(fmt.Errorf("metadata: store failed: %w", err))
	}
}

// owner is the module that handles operation, and how it treats it.
func (a *DB) owner(operation runtime.ControlPlaneOperation) (module.Module, module.Mode) {
	for _, m := range a.modules {
		if mode := m.Mode(operation); mode != module.NotMine {
			return m, mode
		}
	}
	return nil, module.NotMine
}

// Mode reports how the store treats operation: Logged (a command), ReadOnly, or
// NotMine (an operation no module handles).
func (a *DB) Mode(operation runtime.ControlPlaneOperation) module.Mode {
	_, mode := a.owner(operation)
	return mode
}

// Write applies a Logged operation at log slot slot, in its own transaction.
// ErrInvalid if no module handles it as a command.
func (a *DB) Write(slot int64, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	m, mode := a.owner(operation)
	if mode != module.Logged {
		return runtime.ControlPlaneDone{Err: runtime.ErrInvalid}, nil
	}
	var d runtime.ControlPlaneDone
	err := a.keyValue.Update(func(transaction engine.Txn) { d = m.Write(transaction, slot, operation) })
	return d, err
}

// Read answers a ReadOnly operation in a read transaction. ErrInvalid if no
// module handles it as a read.
func (a *DB) Read(operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	m, mode := a.owner(operation)
	if mode != module.ReadOnly {
		return runtime.ControlPlaneDone{Err: runtime.ErrInvalid}, nil
	}
	var d runtime.ControlPlaneDone
	err := a.keyValue.View(func(transaction engine.Txn) { d = m.Read(transaction, operation) })
	return d, err
}
