package controlplanetest

import (
	"slices"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// The test client turns contract operations into generated api/v1 requests and
// the answers back into contract values. It is test support only: the server has
// its own conversions, in the other direction (server/topics.go and friends).

// errCodePairs is the one list that ties each runtime error code to its wire code.
// The test client reads the wire to core direction. To add a code, add one line here, plus the new
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

func pendingBatchFromCore(b contract.PendingBatch) *cpv1.PendingBatch {
	return &cpv1.PendingBatch{
		Tp:      topicPartitionFromCore(b.TopicPartition),
		Records: b.Records,
		Loc:     segmentRefFromCore(b.Loc),
	}
}

func batchRangeToCore(br *cpv1.BatchRange) contract.BatchRange {
	return contract.BatchRange{
		TopicPartition: topicPartitionToCore(br.GetTp()),
		Base:           contract.Offset(br.GetBase()),
		Count:          br.GetCount(),
		Loc:            segmentRefToCore(br.GetLoc()),
	}
}

func topicInfoToCore(t *cpv1.TopicInfo) contract.TopicInfo {
	return contract.TopicInfo{Name: t.GetName(), Partitions: t.GetPartitions()}
}

// commitSegmentRequest builds a CommitSegmentRequest from a contract op.
func commitSegmentRequest(operation contract.CommitSegment) *cpv1.CommitSegmentRequest {
	return &cpv1.CommitSegmentRequest{
		SegmentKey: operation.SegmentKey,
		Batches:    mapSlice(operation.Batches, pendingBatchFromCore),
	}
}

// lookupRequest builds a LookupRequest from a contract op.
func lookupRequest(operation contract.FindSegments) *cpv1.LookupRequest {
	return &cpv1.LookupRequest{
		Tp:       topicPartitionFromCore(operation.TopicPartition),
		From:     int64(operation.From),
		MaxBytes: operation.MaxBytes,
	}
}

// describeTopicsRequest builds a DescribeTopicsRequest from a contract op.
func describeTopicsRequest(operation contract.DescribeTopics) *cpv1.DescribeTopicsRequest {
	return &cpv1.DescribeTopicsRequest{Topics: slices.Clone(operation.Topics)}
}

// commitSegmentAnswer rebuilds a contract.CommitSegmentResp from a response.
func commitSegmentAnswer(r *cpv1.CommitSegmentResponse) contract.CommitSegmentResp {
	return contract.CommitSegmentResp{Assigned: mapSlice(r.GetAssigned(), batchRangeToCore)}
}

// lookupAnswer rebuilds a contract.FindSegmentsResp from a response.
func lookupAnswer(r *cpv1.LookupResponse) contract.FindSegmentsResp {
	return contract.FindSegmentsResp{End: contract.Offset(r.GetEnd()), Ranges: mapSlice(r.GetRanges(), batchRangeToCore)}
}

// describeTopicsAnswer rebuilds a contract.DescribeTopicsResp from a response.
func describeTopicsAnswer(r *cpv1.DescribeTopicsResponse) contract.DescribeTopicsResp {
	return contract.DescribeTopicsResp{Topics: mapSlice(r.GetTopics(), topicInfoToCore)}
}

// toCoreErr maps a wire ErrCode to runtime.ErrCode. Unknown values become ErrIO.
func toCoreErr(c cpv1.ErrCode) runtime.ErrCode {
	for _, pair := range errCodePairs {
		if pair.wire == c {
			return pair.core
		}
	}
	return runtime.ErrIO
}
