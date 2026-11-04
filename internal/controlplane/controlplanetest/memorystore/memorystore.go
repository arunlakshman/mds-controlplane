// Package memorystore is an in-memory store over the reference model
// (internal/spec) behind a mutex, for local development and handler tests.
package memorystore

import (
	"context"
	"sync"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/spec"
)

// Store is an in-memory store backed by the control plane model.
type Store struct {
	mu    sync.Mutex
	model *spec.ControlPlane
}

// New returns an empty store.
func New() *Store { return &Store{model: spec.New()} }

// Exec runs one operation on the model. A business error is the Err of the
// answer. The error result is for the context.
func (s *Store) Exec(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	if err := ctx.Err(); err != nil {
		return runtime.ControlPlaneDone{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.model.Apply(operation), nil
}
