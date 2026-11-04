// Package memorystore is an in-memory store over the speccheck spec behind a mutex, for
// local development and handler tests.
package memorystore

import (
	"context"
	"sync"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/verify/speccheck"
)

// Store is an in-memory store backed by the speccheck control-plane spec.
type Store struct {
	mu   sync.Mutex
	spec *speccheck.ControlPlane
}

// New returns an empty store.
func New() *Store { return &Store{spec: speccheck.NewControlPlane()} }

// Exec runs one operation on the spec. A business error is the Err of the
// answer. The error result is for the context.
func (s *Store) Exec(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	if err := ctx.Err(); err != nil {
		return runtime.ControlPlaneDone{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if x, ok := operation.(contract.CreateTopic); ok {
		return runtime.ControlPlaneDone{Err: s.spec.CreateTopicIf(x.Name, x.Partitions, x.FailIfExists)}, nil
	}
	return s.spec.Exec(runtime.ControlPlaneCall{Op: operation}).(runtime.ControlPlaneDone), nil
}
