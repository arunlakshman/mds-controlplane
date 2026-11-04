// Package storecall gives tests a typed view of a store's single Exec method.
package storecall

import (
	"context"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Executor is anything with the single Exec method of the test stores.
type Executor = runtime.OperationRunner

// Call runs an operation that has a response and a business code.
func Call[R any](ctx context.Context, s Executor, operation runtime.ControlPlaneOperation) (R, runtime.ErrCode, error) {
	d, err := s.Exec(ctx, operation)
	return runtime.ResponseAs[R](d), d.Err, err
}

// Code runs an operation that has only a business code.
func Code(ctx context.Context, s Executor, operation runtime.ControlPlaneOperation) (runtime.ErrCode, error) {
	d, err := s.Exec(ctx, operation)
	return d.Err, err
}

// Read runs an operation that has a response and no business code.
func Read[R any](ctx context.Context, s Executor, operation runtime.ControlPlaneOperation) (R, error) {
	d, err := s.Exec(ctx, operation)
	return runtime.ResponseAs[R](d), err
}
