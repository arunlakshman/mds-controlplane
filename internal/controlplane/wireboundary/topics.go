package wireboundary

import (
	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"slices"
)

// Conversions for the topics context: CommitSegment, Lookup and DescribeTopics.

func topicInfoFromCore(t contract.TopicInfo) *cpv1.TopicInfo {
	return &cpv1.TopicInfo{Name: t.Name, Partitions: t.Partitions}
}

func topicInfoToCore(t *cpv1.TopicInfo) contract.TopicInfo {
	return contract.TopicInfo{Name: t.GetName(), Partitions: t.GetPartitions()}
}

// CommitSegmentFromCore builds a CommitSegmentRequest from a contract op.
func CommitSegmentFromCore(operation contract.CommitSegment) *cpv1.CommitSegmentRequest {
	return &cpv1.CommitSegmentRequest{
		SegmentKey: operation.SegmentKey,
		Batches:    mapSlice(operation.Batches, pendingBatchFromCore),
	}
}

// CommitSegmentToCore rebuilds a contract.CommitSegment from a request.
func CommitSegmentToCore(request *cpv1.CommitSegmentRequest) contract.CommitSegment {
	return contract.CommitSegment{
		SegmentKey: request.GetSegmentKey(),
		Batches:    mapSlice(request.GetBatches(), pendingBatchToCore),
	}
}

// LookupFromCore builds a LookupRequest from a contract op.
func LookupFromCore(operation contract.FindSegments) *cpv1.LookupRequest {
	return &cpv1.LookupRequest{
		Tp:       topicPartitionFromCore(operation.TopicPartition),
		From:     int64(operation.From),
		MaxBytes: operation.MaxBytes,
	}
}

// LookupToCore rebuilds a contract.FindSegments from a request.
func LookupToCore(request *cpv1.LookupRequest) contract.FindSegments {
	return contract.FindSegments{
		TopicPartition: topicPartitionToCore(request.GetTp()),
		From:           contract.Offset(request.GetFrom()),
		MaxBytes:       request.GetMaxBytes(),
	}
}

// DescribeTopicsFromCore builds a DescribeTopicsRequest from a contract op.
func DescribeTopicsFromCore(operation contract.DescribeTopics) *cpv1.DescribeTopicsRequest {
	return &cpv1.DescribeTopicsRequest{Topics: slices.Clone(operation.Topics)}
}

// DescribeTopicsToCore rebuilds a contract.DescribeTopics from a request.
func DescribeTopicsToCore(request *cpv1.DescribeTopicsRequest) contract.DescribeTopics {
	return contract.DescribeTopics{Topics: slices.Clone(request.GetTopics())}
}

// CommitSegmentRespFromCore builds a CommitSegmentResponse from a contract response.
func CommitSegmentRespFromCore(r contract.CommitSegmentResp) *cpv1.CommitSegmentResponse {
	return &cpv1.CommitSegmentResponse{Assigned: mapSlice(r.Assigned, batchRangeFromCore)}
}

// CommitSegmentRespToCore rebuilds a contract.CommitSegmentResp from a response.
func CommitSegmentRespToCore(r *cpv1.CommitSegmentResponse) contract.CommitSegmentResp {
	return contract.CommitSegmentResp{Assigned: mapSlice(r.GetAssigned(), batchRangeToCore)}
}

// LookupRespFromCore builds a LookupResponse from a contract response.
func LookupRespFromCore(r contract.FindSegmentsResp) *cpv1.LookupResponse {
	return &cpv1.LookupResponse{End: int64(r.End), Ranges: mapSlice(r.Ranges, batchRangeFromCore)}
}

// LookupRespToCore rebuilds a contract.FindSegmentsResp from a response.
func LookupRespToCore(r *cpv1.LookupResponse) contract.FindSegmentsResp {
	return contract.FindSegmentsResp{End: contract.Offset(r.GetEnd()), Ranges: mapSlice(r.GetRanges(), batchRangeToCore)}
}

// DescribeTopicsRespFromCore builds a DescribeTopicsResponse from a contract response.
func DescribeTopicsRespFromCore(r contract.DescribeTopicsResp) *cpv1.DescribeTopicsResponse {
	return &cpv1.DescribeTopicsResponse{Topics: mapSlice(r.Topics, topicInfoFromCore)}
}

// DescribeTopicsRespToCore rebuilds a contract.DescribeTopicsResp from a response.
func DescribeTopicsRespToCore(r *cpv1.DescribeTopicsResponse) contract.DescribeTopicsResp {
	return contract.DescribeTopicsResp{Topics: mapSlice(r.GetTopics(), topicInfoToCore)}
}

// ---- Broker heartbeat ----
