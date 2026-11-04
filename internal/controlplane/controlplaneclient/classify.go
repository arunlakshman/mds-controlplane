package controlplaneclient

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"syscall"

	"google.golang.org/grpc/codes"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/system"
)

// Classifier is the system.ErrorClassifier for ControlPlaneCall (lesson 1). A mutating op
// (CommitSegment) whose request may have reached the server and whose reply
// is lost or unusable is MaybeApplied; only failures that prove nothing was
// sent (dial, Unavailable) are NotApplied. Reads are always NotApplied.
// Cancel/deadline without a prior success is ErrTimeout + MaybeApplied
// (Appendix E).
type Classifier struct{}

var _ system.ErrorClassifier = Classifier{}

func changesData(a runtime.IOCall) bool {
	call, ok := a.(runtime.ControlPlaneCall)
	if !ok {
		return true
	}
	switch call.Op.(type) {
	case contract.FindSegments, contract.DescribeTopics, contract.OffsetFetch, contract.DescribeGroups, contract.ListGroups:
		return false
	}
	return true
}

// Classify maps a failed cp call to its error code and outcome.
func (Classifier) Classify(a runtime.IOCall, err error) (runtime.ErrCode, runtime.Outcome) {
	code, out := classify(a, err)
	slog.Debug("cp call failed", "action", a, "err", err, "code", code, "outcome", out)
	return code, out
}

func classify(a runtime.IOCall, err error) (runtime.ErrCode, runtime.Outcome) {
	maybe := runtime.NotApplied
	if changesData(a) {
		maybe = runtime.MaybeApplied
	}
	if be, ok := errors.AsType[*Error](err); ok {
		out := be.Outcome
		if out == 0 && be.Code != runtime.OK {
			out = runtime.NotApplied
		}
		return be.Code, out
	}
	if ce, ok := errors.AsType[*CallError](err); ok {
		return classifyCall(ce, maybe)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return runtime.ErrTimeout, maybe
	}
	return isTimeoutOrIOError(err), maybe
}

func classifyCall(ce *CallError, maybe runtime.Outcome) (runtime.ErrCode, runtime.Outcome) {
	switch {
	case ce.Code == codes.Unavailable:
		return runtime.ErrUnavailable, runtime.NotApplied
	case ce.Code == codes.InvalidArgument:
		return runtime.ErrInvalid, runtime.NotApplied
	case ce.Code == codes.Canceled || ce.Code == codes.DeadlineExceeded:
		return runtime.ErrTimeout, maybe
	case !ce.Sent:
		return classifyBeforeSend(ce.Err), runtime.NotApplied
	}
	return isTimeoutOrIOError(ce.Err), maybe
}

func classifyBeforeSend(err error) runtime.ErrCode {
	if isDial(err) {
		return runtime.ErrUnavailable
	}
	return isTimeoutOrIOError(err)
}

func isTimeoutOrIOError(err error) runtime.ErrCode {
	ne, isNetErr := errors.AsType[net.Error](err)
	if errors.Is(err, context.DeadlineExceeded) || (isNetErr && ne.Timeout()) {
		return runtime.ErrTimeout
	}
	return runtime.ErrIO
}

func isDial(err error) bool {
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return true
	}
	if operation, ok := errors.AsType[*net.OpError](err); ok && operation.Op == "dial" {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH)
}
