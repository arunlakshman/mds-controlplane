package server

import (
	"context"
	"errors"
	"slices"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadata"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// The topics RPCs: CommitSegment, Lookup, DescribeTopics and CreateTopic. Each
// one is a module operation. It converts the request, runs the operation with
// runOperation (a command goes to the log, a read is answered by the fold), and
// converts the answer.

// isCommand reports whether operation is written to the log. Any other
// operation is a read, answered by the fold.
func isCommand(operation runtime.ControlPlaneOperation) bool {
	_, logged := metadata.EntryOf(operation)
	return logged
}

// Exec runs one operation and is the only place that splits commands from reads.
// A command is sent to the replica as a ControlPlaneDeliver and answered after
// its slot applies. A read is sent as a LocalRead and answered at once by the
// fold of this node.
// A business error is the Err of the answer; the error result is for the run
// loop and the context. Node is the log and read side of the group coordinator
// (through nodeGroups) and of every topics RPC.
func (n *Node) Exec(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	if _, logged := metadata.EntryOf(operation); logged {
		return n.ask(ctx, true, func(request runtime.RequestID) runtime.EventData {
			return runtime.ControlPlaneDeliver{From: serverFrom, Req: request, Op: operation}
		})
	}
	return n.ask(ctx, false, func(request runtime.RequestID) runtime.EventData {
		return runtime.LocalRead{From: serverFrom, Req: request, Op: operation}
	})
}

var _ runtime.OperationRunner = (*Node)(nil)

// runOperation runs one module operation for an RPC and maps a failure to the
// RPC error. A call that ends with its context may have been applied, so for a
// command it is MaybeApplied. A failed read changed nothing, so it is NotApplied.
func (n *Node) runOperation(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	d, err := n.Exec(ctx, operation)
	if err != nil {
		if !isCommand(operation) {
			return d, toRPCError(runtime.ErrIO, runtime.NotApplied)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return d, toRPCError(runtime.ErrTimeout, runtime.MaybeApplied)
		}
		return d, toRPCError(runtime.ErrIO, runtime.MaybeApplied)
	}
	return d, answerError(d)
}

// answerError is the RPC error of a business answer that failed, or nil.
func answerError(d runtime.ControlPlaneDone) error {
	if d.Err != runtime.OK {
		return toRPCError(d.Err, d.Outcome)
	}
	return nil
}

// moduleRPC runs one module operation and converts the answer, as R, to the wire
// type Out.
func moduleRPC[R, Out any](ctx context.Context, n *Node, operation runtime.ControlPlaneOperation, convert func(R) Out) (Out, error) {
	var zero Out
	d, err := n.runOperation(ctx, operation)
	if err != nil {
		return zero, err
	}
	return convert(runtime.ResponseAs[R](d)), nil
}

// CommitSegment implements ControlPlane.
func (n *Node) CommitSegment(ctx context.Context, request *cpv1.CommitSegmentRequest) (*cpv1.CommitSegmentResponse, error) {
	return moduleRPC(ctx, n, commitSegmentToCore(request), commitSegmentRespFromCore)
}

// Lookup implements ControlPlane.
func (n *Node) Lookup(ctx context.Context, request *cpv1.LookupRequest) (*cpv1.LookupResponse, error) {
	return moduleRPC(ctx, n, lookupToCore(request), lookupRespFromCore)
}

// DescribeTopics implements ControlPlane.
func (n *Node) DescribeTopics(ctx context.Context, request *cpv1.DescribeTopicsRequest) (*cpv1.DescribeTopicsResponse, error) {
	return moduleRPC(ctx, n, describeTopicsToCore(request), describeTopicsRespFromCore)
}

// CreateTopic implements ControlPlane.
func (n *Node) CreateTopic(ctx context.Context, request *cpv1.CreateTopicRequest) (*cpv1.CreateTopicResponse, error) {
	if request == nil {
		return nil, toRPCError(runtime.ErrInvalid, runtime.NotApplied)
	}
	operation := contract.CreateTopic{Name: request.Topic, Partitions: request.Partitions, FailIfExists: request.FailIfExists}
	return moduleRPC(ctx, n, operation, func(runtime.ControlPlaneResponse) *cpv1.CreateTopicResponse { return &cpv1.CreateTopicResponse{} })
}

// ---- Wire conversions ----
//
// The handlers above read generated api/v1 messages and build contract values
// with these functions, and build the answers back. Each reads the message with
// its Get methods, so a nil or missing part is a zero value and never a panic.

// Conversions for the topics context: CommitSegment, Lookup and DescribeTopics.

func topicInfoFromCore(t contract.TopicInfo) *cpv1.TopicInfo {
	return &cpv1.TopicInfo{Name: t.Name, Partitions: t.Partitions}
}

// commitSegmentToCore rebuilds a contract.CommitSegment from a request.
func commitSegmentToCore(request *cpv1.CommitSegmentRequest) contract.CommitSegment {
	return contract.CommitSegment{
		SegmentKey: request.GetSegmentKey(),
		Batches:    mapSlice(request.GetBatches(), pendingBatchToCore),
	}
}

// lookupToCore rebuilds a contract.FindSegments from a request.
func lookupToCore(request *cpv1.LookupRequest) contract.FindSegments {
	return contract.FindSegments{
		TopicPartition: topicPartitionToCore(request.GetTp()),
		From:           contract.Offset(request.GetFrom()),
		MaxBytes:       request.GetMaxBytes(),
	}
}

// describeTopicsToCore rebuilds a contract.DescribeTopics from a request.
func describeTopicsToCore(request *cpv1.DescribeTopicsRequest) contract.DescribeTopics {
	return contract.DescribeTopics{Topics: slices.Clone(request.GetTopics())}
}

// commitSegmentRespFromCore builds a CommitSegmentResponse from a contract response.
func commitSegmentRespFromCore(r contract.CommitSegmentResp) *cpv1.CommitSegmentResponse {
	return &cpv1.CommitSegmentResponse{Assigned: mapSlice(r.Assigned, batchRangeFromCore)}
}

// lookupRespFromCore builds a LookupResponse from a contract response.
func lookupRespFromCore(r contract.FindSegmentsResp) *cpv1.LookupResponse {
	return &cpv1.LookupResponse{End: int64(r.End), Ranges: mapSlice(r.Ranges, batchRangeFromCore)}
}

// describeTopicsRespFromCore builds a DescribeTopicsResponse from a contract response.
func describeTopicsRespFromCore(r contract.DescribeTopicsResp) *cpv1.DescribeTopicsResponse {
	return &cpv1.DescribeTopicsResponse{Topics: mapSlice(r.Topics, topicInfoFromCore)}
}

// ---- Broker heartbeat ----
