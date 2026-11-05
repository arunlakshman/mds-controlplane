package server

import (
	"context"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/wire"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func toRPCError(code runtime.ErrCode, outcome runtime.Outcome) error {
	st := codes.Internal
	switch code {
	case runtime.ErrUnavailable:
		st = codes.Unavailable
	case runtime.ErrInvalid:
		st = codes.InvalidArgument
	case runtime.ErrNotFound:
		st = codes.NotFound
	case runtime.ErrConflict, runtime.ErrTopicExists:
		st = codes.AlreadyExists
	case runtime.ErrTimeout:
		st = codes.DeadlineExceeded
	case runtime.ErrTooLarge:
		st = codes.ResourceExhausted
	case runtime.ErrUnknownMember, runtime.ErrUnknownPartition:
		st = codes.NotFound
	case runtime.ErrIllegalGeneration, runtime.ErrRebalanceInProgress, runtime.ErrInconsistentGroupProtocol:
		st = codes.FailedPrecondition
	case runtime.OK:
		return nil
	}
	detail := &cpv1.ErrorDetail{
		Code:    wire.ErrCodeFromCore(code),
		Outcome: fromCoreOutcome(outcome),
	}
	s, err := status.New(st, code.String()).WithDetails(detail)
	if err != nil {
		return status.Error(st, code.String())
	}
	return s.Err()
}

// GetStatus implements ControlPlane.
func (n *Node) GetStatus(ctx context.Context, _ *cpv1.GetStatusRequest) (*cpv1.GetStatusResponse, error) {
	type st struct {
		role             string
		applied, logTail int64
		epoch, stepDowns uint64
		ready            bool
	}
	ch := make(chan st, 1)
	err := n.onLoop(ctx, func(m *replica.Replica) []any {
		role := "standby"
		if m.IsActive() {
			role = "active"
		}
		ch <- st{
			role: role, applied: int64(m.Applied()), logTail: int64(m.Applied()),
			epoch: m.Epoch(), stepDowns: m.StepDowns(), ready: m.Ready(),
		}
		return nil
	})
	if err != nil {
		return nil, toRPCError(runtime.ErrIO, runtime.NotApplied)
	}
	select {
	case s := <-ch:
		if !s.ready {
			return nil, toRPCError(runtime.ErrUnavailable, runtime.NotApplied)
		}
		return &cpv1.GetStatusResponse{Role: s.role, AppliedSlot: s.applied, LogTail: s.logTail,
			Epoch: s.epoch, StepDowns: s.stepDowns}, nil
	case <-ctx.Done():
		return nil, toRPCError(runtime.ErrTimeout, runtime.NotApplied)
	}
}
