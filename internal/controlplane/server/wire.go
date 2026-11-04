package server

import (
	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Conversions shared by every context: error codes and outcomes, and the
// types that more than one context uses (topic partitions, segment references,
// batches and ranges).

// ---- Error and outcome codes ----

// errCodePairs is the one list that ties each runtime error code to its wire code.
// Both directions read it. To add a code, add one line here, plus the new
// constant in runtime and the new value in the proto enum. TestErrCodeTableIsComplete
// fails if a code is missing.
var errCodePairs = [...]struct {
	core runtime.ErrCode
	wire cpv1.ErrCode
}{
	{runtime.OK, cpv1.ErrCode_ERR_CODE_OK},
	{runtime.ErrTimeout, cpv1.ErrCode_ERR_CODE_TIMEOUT},
	{runtime.ErrUnavailable, cpv1.ErrCode_ERR_CODE_UNAVAILABLE},
	{runtime.ErrNotFound, cpv1.ErrCode_ERR_CODE_NOT_FOUND},
	{runtime.ErrConflict, cpv1.ErrCode_ERR_CODE_CONFLICT},
	{runtime.ErrConnClosed, cpv1.ErrCode_ERR_CODE_CONN_CLOSED},
	{runtime.ErrTooLarge, cpv1.ErrCode_ERR_CODE_TOO_LARGE},
	{runtime.ErrCorrupt, cpv1.ErrCode_ERR_CODE_CORRUPT},
	{runtime.ErrInvalid, cpv1.ErrCode_ERR_CODE_INVALID},
	{runtime.ErrIO, cpv1.ErrCode_ERR_CODE_IO},
	{runtime.ErrUnknownMember, cpv1.ErrCode_ERR_CODE_UNKNOWN_MEMBER},
	{runtime.ErrIllegalGeneration, cpv1.ErrCode_ERR_CODE_ILLEGAL_GENERATION},
	{runtime.ErrUnknownPartition, cpv1.ErrCode_ERR_CODE_UNKNOWN_PARTITION},
	{runtime.ErrRebalanceInProgress, cpv1.ErrCode_ERR_CODE_REBALANCE_IN_PROGRESS},
	{runtime.ErrInconsistentGroupProtocol, cpv1.ErrCode_ERR_CODE_INCONSISTENT_GROUP_PROTOCOL},
	{runtime.ErrTopicExists, cpv1.ErrCode_ERR_CODE_TOPIC_EXISTS},
}

// fromCoreErr maps runtime.ErrCode to the wire enum. Unknown values become ERR_CODE_IO.
func fromCoreErr(c runtime.ErrCode) cpv1.ErrCode {
	for _, pair := range errCodePairs {
		if pair.core == c {
			return pair.wire
		}
	}
	return cpv1.ErrCode_ERR_CODE_IO
}

// fromCoreOutcome maps runtime.Outcome to the wire enum.
func fromCoreOutcome(o runtime.Outcome) cpv1.Outcome {
	switch o {
	case runtime.MaybeApplied:
		return cpv1.Outcome_OUTCOME_MAYBE_APPLIED
	case runtime.Applied:
		return cpv1.Outcome_OUTCOME_APPLIED
	default:
		return cpv1.Outcome_OUTCOME_NOT_APPLIED
	}
}

// ---- Shared pieces ----

// Rule for every "ToCore" function in this package: read the wire message with
// its Get methods. They accept a nil message and give the zero value, so a
// missing part becomes a zero value and nothing panics.

// mapSlice converts every element. An empty input gives nil, the same as a
// loop that appends to a nil slice.
func mapSlice[From, To any](in []From, convert func(From) To) []To {
	if len(in) == 0 {
		return nil
	}
	out := make([]To, len(in))
	for i, item := range in {
		out[i] = convert(item)
	}
	return out
}

func topicPartitionFromCore(tp contract.TopicPartition) *cpv1.TopicPartition {
	return &cpv1.TopicPartition{Topic: tp.Topic, Partition: tp.Partition}
}

func topicPartitionToCore(tp *cpv1.TopicPartition) contract.TopicPartition {
	return contract.NewTopicPartition(tp.GetTopic(), tp.GetPartition())
}

func segmentRefFromCore(ref runtime.SegmentRef) *cpv1.SegmentRef {
	return &cpv1.SegmentRef{Key: ref.Key, Off: ref.Off, Len: ref.Len}
}

func segmentRefToCore(ref *cpv1.SegmentRef) runtime.SegmentRef {
	return runtime.SegmentRef{Key: ref.GetKey(), Off: ref.GetOff(), Len: ref.GetLen()}
}

func pendingBatchToCore(b *cpv1.PendingBatch) contract.PendingBatch {
	return contract.PendingBatch{
		TopicPartition: topicPartitionToCore(b.GetTp()),
		Records:        b.GetRecords(),
		Loc:            segmentRefToCore(b.GetLoc()),
	}
}

func batchRangeFromCore(br contract.BatchRange) *cpv1.BatchRange {
	return &cpv1.BatchRange{
		Tp:    topicPartitionFromCore(br.TopicPartition),
		Base:  int64(br.Base),
		Count: br.Count,
		Loc:   segmentRefFromCore(br.Loc),
	}
}
