package controlplanetest

import (
	"errors"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/controlplaneclient"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/verify/speccheck/checker/generator"
	"github.com/manifesterio/mds-controlplane/internal/verify/speccheck/checker/observation"
	"github.com/manifesterio/mds-controlplane/internal/verify/speccheck/checker/suites"
)

// compareSteps runs the operations one by one on the subject and on a fresh
// spec. It fails at the first difference.
func compareSteps(t *testing.T, subject suites.ControlPlaneSubject, ops []generator.ControlPlaneTestStep) {
	t.Helper()
	spec := suites.NewSpecControlPlaneSubject()
	for i, operation := range ops {
		expected, err := spec.Do(operation)
		if err != nil {
			t.Fatalf("The spec could not run operation %d (%s): %v", i, operation.Describe(), err)
		}
		actual, err := subject.Do(operation)
		if err != nil {
			t.Fatalf("The subject could not run operation %d (%s): %v", i, operation.Describe(), err)
		}
		if operation.CreateTopic != nil {
			expected, actual = observation.ControlPlaneResult{Code: expected.Code}, observation.ControlPlaneResult{Code: actual.Code}
		}
		if diff := observation.DiffControlPlane(expected, actual); diff != "" {
			t.Fatalf("For operation %d (%s): %s", i, operation.Describe(), diff)
		}
	}
}

// call wraps a control-plane operation. The request number is not used by the
// stores, so it is always 1.
func call(operation runtime.ControlPlaneOperation) generator.ControlPlaneTestStep {
	return generator.NewCall(runtime.ControlPlaneCall{Req: 1, Op: operation})
}

// replayOps commits a three-batch, two-partition segment, replays it after the
// ends moved, sends a different request under its key, an invalid request, and
// a retry of the same batches under a fresh key (new offsets).
func replayOps() []generator.ControlPlaneTestStep {
	a, b := contract.NewTopicPartition("w", 0), contract.NewTopicPartition("w", 1)
	batches := func(key string) []contract.PendingBatch {
		return []contract.PendingBatch{
			{TopicPartition: a, Records: 2, Loc: runtime.SegmentRef{Key: key, Off: 0, Len: 20}},
			{TopicPartition: b, Records: 5, Loc: runtime.SegmentRef{Key: key, Off: 20, Len: 50}},
			{TopicPartition: a, Records: 3, Loc: runtime.SegmentRef{Key: key, Off: 70, Len: 30}},
		}
	}
	commit := func(key string, bs []contract.PendingBatch) generator.ControlPlaneTestStep {
		return call(contract.CommitSegment{SegmentKey: key, Batches: bs})
	}
	return []generator.ControlPlaneTestStep{
		commit("s1", batches("s1")),
		commit("s2", batches("s2")),
		commit("s1", batches("s1")),                                // replay: the original ranges
		commit("s1", batches("s2")),                                // other request under s1: conflict
		commit("s1", nil),                                          // malformed under a committed key: conflict
		commit("s3", []contract.PendingBatch{{TopicPartition: a}}), // invalid: no records
		commit("s3", batches("s3")),                                // the failed key is reusable
		commit("re", batches("s3")),                                // the same batches, a fresh key: new offsets
		call(contract.FindSegments{TopicPartition: a}), call(contract.FindSegments{TopicPartition: b, From: 6, MaxBytes: 25}),
		call(contract.DescribeTopics{}),
	}
}

func TestReplayMemorystore(t *testing.T) {
	compareSteps(t, implSubject{newMem()}, replayOps())
}

// manyBatchesOps commits segments of many small batches (one DynamoDB range
// item per partition and segment), with a replay
// of a segment, a segment spanning two runs of one partition (two commits), and
// a Lookup from every offset with and without a byte limit.
func manyBatchesOps() []generator.ControlPlaneTestStep {
	var ops []generator.ControlPlaneTestStep
	a := contract.NewTopicPartition("m", 0)
	b := contract.NewTopicPartition("m", 1)
	var s1, s2 []contract.PendingBatch
	for i := range 200 { // 200 batches for a, interleaved with b
		for _, topicPartition := range []contract.TopicPartition{a, b} {
			if topicPartition == b && i%4 != 0 {
				continue
			}
			s1 = append(s1, contract.PendingBatch{TopicPartition: topicPartition, Records: int64(1 + i%3),
				Loc: runtime.SegmentRef{Key: "big1", Off: int64(i) * 70, Len: int64(10 + i%5)}})
		}
	}
	// Several batches of one partition around one of another.
	s2 = []contract.PendingBatch{
		{TopicPartition: a, Records: 2, Loc: runtime.SegmentRef{Key: "big2", Off: 0, Len: 9}},
		{TopicPartition: a, Records: 2, Loc: runtime.SegmentRef{Key: "big2", Off: 9, Len: 9}},
		{TopicPartition: a, Records: 3, Loc: runtime.SegmentRef{Key: "big2", Off: 18, Len: 9}},
		{TopicPartition: b, Records: 1, Loc: runtime.SegmentRef{Key: "big2", Off: 27, Len: 9}},
		{TopicPartition: a, Records: 1, Loc: runtime.SegmentRef{Key: "big2", Off: 36, Len: 9}},
	}
	commit := func(key string, bs []contract.PendingBatch) generator.ControlPlaneTestStep {
		return call(contract.CommitSegment{SegmentKey: key, Batches: bs})
	}
	ops = append(ops, commit("big1", s1), commit("big1", s1), commit("big2", s2), commit("big2", s2),
		commit("big3", s1), // a second run on each partition, right after the first
		commit("big1", s2)) // same key, other batches: conflict
	for _, topicPartition := range []contract.TopicPartition{a, b, contract.NewTopicPartition("m", 2)} {
		for from := int64(0); from <= 520; from += 7 {
			for _, mb := range []int32{0, 1, 25, 300} {
				ops = append(ops, call(contract.FindSegments{TopicPartition: topicPartition, From: contract.Offset(from), MaxBytes: mb}))
			}
		}
	}
	return append(ops, call(contract.DescribeTopics{}))
}

func TestManyBatchesMemorystore(t *testing.T) {
	compareSteps(t, implSubject{newMem()}, manyBatchesOps())
}

func TestRoundTrip(t *testing.T) {
	ctx := t.Context()
	n := StartMem(t)
	c := n.Client
	if err := c.CreateTopic(ctx, "t", 3); err != nil {
		t.Fatal(err)
	}
	commit := contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{
		{TopicPartition: contract.NewTopicPartition("t", 1), Records: 4, Loc: runtime.SegmentRef{Key: "k", Off: 8, Len: 40}},
	}}
	cr, err := c.Call(ctx, commit)
	if err != nil {
		t.Fatal(err)
	}
	if got := cr.(contract.CommitSegmentResp).Assigned; len(got) != 1 || got[0].Base != 0 || got[0].Count != 4 || got[0].Loc != commit.Batches[0].Loc {
		t.Fatalf("commit: %+v", got)
	}
	l, err := c.Call(ctx, contract.FindSegments{TopicPartition: commit.Batches[0].TopicPartition})
	if err != nil || l.(contract.FindSegmentsResp).End != 4 || len(l.(contract.FindSegmentsResp).Ranges) != 1 {
		t.Fatalf("lookup: %+v %v", l, err)
	}
	d, err := c.Call(ctx, contract.DescribeTopics{})
	if err != nil || d.(contract.DescribeTopicsResp).Topics[0] != (contract.TopicInfo{Name: "t", Partitions: 3}) {
		t.Fatalf("describe: %+v %v", d, err)
	}
	commit.Batches[0].Records = 9
	_, err = c.Call(ctx, commit)
	be, ok := errors.AsType[*controlplaneclient.Error](err)
	if !ok || be.Code != runtime.ErrConflict {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	if code, out := (controlplaneclient.Classifier{}).Classify(runtime.ControlPlaneCall{Op: commit}, err); code != runtime.ErrConflict || out != runtime.NotApplied {
		t.Fatalf("classify: %v %v", code, out)
	}
}
