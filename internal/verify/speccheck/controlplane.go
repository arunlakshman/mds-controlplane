package speccheck

import (
	"slices"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// ControlPlane is the spec of the control plane as a broker sees it.
// Wire payloads are controlplane/api/v1 messages (decision 57); this oracle
// still steps runtime.ControlPlaneOperation values via ControlPlaneCall so sim and brokers keep one bridge,
// with apiconv at the gRPC edge. Apply logic stays unshared (decision 21).
// Sequential, fault-free; Outcome is Applied for a state change and NotApplied
// for errors and reads. Rules:
//
//	Log            A partition's log is a list of runs (contract.BatchRange), each
//	               [Base, Base+Count). Its end offset is the newest run's
//	               Base+Count, 0 when empty. There is no separate counter.
//	CommitSegment  1. SegmentKey already committed: the original ranges again
//	                  (OK, Applied); ErrConflict if the request differs.
//	               2. Empty key or batches, Records <= 0, empty topic or
//	                  negative partition: ErrInvalid, no change.
//	               3. Else every batch gets Base = its partition's end offset
//	                  (newest run's Base+Count, 0 if empty), in request order,
//	                  the end then growing by Records: offsets are contiguous
//	                  per partition, all partitions are assigned in one step,
//	                  all or nothing. Topics are created implicitly and grow
//	                  to cover the partition.
//	               A failed commit records nothing: its key is reusable.
//	               The real applier runs commands one at a time in log order,
//	               so it needs no locking for step 3. The spec is sequential too.
//	Lookup         From < 0: ErrInvalid. Else the runs of TopicPartition whose last offset
//	               is >= From, in offset order, plus the end offset. MaxBytes > 0
//	               stops after the batch that takes the summed Loc.Len to
//	               MaxBytes or more, but always returns one if any match. An
//	               unknown partition is empty, End 0.
//	DescribeTopics sorted by name; empty request = all; unknown names are
//	               omitted.
//	CreateTopic    ErrInvalid for an empty name or partitions <= 0. With
//	               FailIfExists, an existing topic is ErrTopicExists and no
//	               change. Otherwise it is OK and never shrinks the topic.
//
// Topics can also be made with CreateTopic. Exec panics on a non-ControlPlaneCall.
type ControlPlane struct {
	segs   map[string]segment
	parts  map[string]map[int32][]contract.BatchRange // topic -> partition -> runs
	topics map[string]int32                           // topic -> partition count

	// segKeys is every committed segment key, sorted. segPre[i] is the
	// StateHash state after segKeys[:i]; segPre[0] is the empty state. A
	// commit at index i truncates it to i+1 entries.
	segKeys []string
	segPre  []stateHasher

	groups  map[string]*groupState                                 // consumer groups by name
	offsets map[string]map[contract.TopicPartition]committedOffset // committed offsets by group
}

type segment struct {
	batches []contract.PendingBatch
	ranges  []contract.BatchRange
}

// NewControlPlane returns an empty control plane.
func NewControlPlane() *ControlPlane {
	return &ControlPlane{
		segs:   map[string]segment{},
		parts:  map[string]map[int32][]contract.BatchRange{},
		topics: map[string]int32{},
	}
}

// CreateTopic creates name with at least partitions partitions (it never
// shrinks one). ErrInvalid for an empty name or partitions <= 0.
func (c *ControlPlane) CreateTopic(name string, partitions int32) runtime.ErrCode {
	return c.CreateTopicIf(name, partitions, false)
}

// CreateTopicIf is CreateTopic. With failIfExists, a topic that is already
// there gives ErrTopicExists and nothing changes. The check comes after the
// ErrInvalid check.
func (c *ControlPlane) CreateTopicIf(name string, partitions int32, failIfExists bool) runtime.ErrCode {
	if name == "" || partitions <= 0 {
		return runtime.ErrInvalid
	}
	if _, there := c.topics[name]; there && failIfExists {
		return runtime.ErrTopicExists
	}
	c.topics[name] = max(c.topics[name], partitions)
	return runtime.OK
}

// End is the next offset of tp: the end of its newest run (0 if unknown).
func (c *ControlPlane) End(topicPartition contract.TopicPartition) contract.Offset {
	rs := c.parts[topicPartition.Topic][topicPartition.Partition]
	if len(rs) == 0 {
		return 0
	}
	last := rs[len(rs)-1]
	return last.Base + contract.Offset(last.Count)
}

// Exec runs one ControlPlaneCall and returns its ControlPlaneDone.
func (c *ControlPlane) Exec(a runtime.IOCall) runtime.IOResult {
	call, ok := a.(runtime.ControlPlaneCall)
	if !ok {
		panic("speccheck.ControlPlane: not a CPCall")
	}
	switch operation := call.Op.(type) {
	case contract.CommitSegment:
		return c.execCommit(call.Req, operation)
	case contract.FindSegments:
		return c.execLookup(call.Req, operation)
	case contract.DescribeTopics:
		return c.execDescribe(call.Req, operation)
	case contract.CreateTopic:
		return c.execCreate(call.Req, operation)
	case contract.GroupInstall:
		return groupDone(call.Req, c.groupInstall(operation))
	case contract.GroupAssign:
		return groupDone(call.Req, c.groupAssign(operation))
	case contract.GroupRemove:
		return groupDone(call.Req, c.groupRemove(operation))
	case contract.OffsetCommit:
		return c.offsetCommit(call.Req, operation)
	case contract.OffsetFetch:
		return runtime.ControlPlaneDone{Req: call.Req, Resp: c.offsetFetch(operation)}
	case contract.DescribeGroup:
		return runtime.ControlPlaneDone{Req: call.Req, Resp: c.describeGroup(operation)}
	case contract.ListGroups:
		return runtime.ControlPlaneDone{Req: call.Req, Resp: c.listGroups()}
	}
	return runtime.ControlPlaneDone{Req: call.Req, Err: runtime.ErrInvalid}
}

func (c *ControlPlane) execCreate(request runtime.RequestID, operation contract.CreateTopic) runtime.IOResult {
	if code := c.CreateTopicIf(operation.Name, operation.Partitions, operation.FailIfExists); code != runtime.OK {
		return runtime.ControlPlaneDone{Req: request, Err: code}
	}
	return runtime.ControlPlaneDone{Req: request, Outcome: runtime.Applied}
}

// validBatch reports whether b is well formed on its own.
func validBatch(b contract.PendingBatch) bool {
	return b.Records > 0 && b.TopicPartition.Topic != "" && b.TopicPartition.Partition >= 0
}

func (c *ControlPlane) execCommit(request runtime.RequestID, operation contract.CommitSegment) runtime.IOResult {
	if committed, done := c.segs[operation.SegmentKey]; done {
		return c.replayCommit(request, operation, committed)
	}
	return c.freshCommit(request, operation)
}

func (c *ControlPlane) replayCommit(request runtime.RequestID, operation contract.CommitSegment, committed segment) runtime.IOResult {
	if !slices.Equal(committed.batches, operation.Batches) {
		return runtime.ControlPlaneDone{Req: request, Err: runtime.ErrConflict}
	}
	response := contract.CommitSegmentResp{Assigned: slices.Clone(committed.ranges)}
	return runtime.ControlPlaneDone{Req: request, Resp: response, Outcome: runtime.Applied}
}

func (c *ControlPlane) freshCommit(request runtime.RequestID, operation contract.CommitSegment) runtime.IOResult {
	if operation.SegmentKey == "" || len(operation.Batches) == 0 {
		return runtime.ControlPlaneDone{Req: request, Err: runtime.ErrInvalid}
	}
	for _, b := range operation.Batches {
		if !validBatch(b) {
			return runtime.ControlPlaneDone{Req: request, Err: runtime.ErrInvalid}
		}
	}
	newSegment := segment{batches: slices.Clone(operation.Batches)}
	for _, b := range operation.Batches {
		r := contract.BatchRange{TopicPartition: b.TopicPartition, Base: c.End(b.TopicPartition), Count: b.Records, Loc: b.Loc}
		c.append(r)
		newSegment.ranges = append(newSegment.ranges, r)
	}
	i, _ := slices.BinarySearch(c.segKeys, operation.SegmentKey)
	c.segKeys = slices.Insert(c.segKeys, i, operation.SegmentKey)
	if len(c.segPre) > i+1 {
		c.segPre = c.segPre[:i+1]
	}
	c.segs[operation.SegmentKey] = newSegment
	response := contract.CommitSegmentResp{Assigned: slices.Clone(newSegment.ranges)}
	return runtime.ControlPlaneDone{Req: request, Resp: response, Outcome: runtime.Applied}
}

func (c *ControlPlane) execLookup(request runtime.RequestID, operation contract.FindSegments) runtime.IOResult {
	if operation.From < 0 {
		return runtime.ControlPlaneDone{Req: request, Err: runtime.ErrInvalid}
	}
	response := contract.FindSegmentsResp{End: c.End(operation.TopicPartition)}
	var bytes int64
	for _, r := range c.parts[operation.TopicPartition.Topic][operation.TopicPartition.Partition] {
		if r.Base+contract.Offset(r.Count) <= operation.From {
			continue
		}
		response.Ranges = append(response.Ranges, r)
		bytes += r.Loc.Len
		if operation.MaxBytes > 0 && bytes >= int64(operation.MaxBytes) {
			break
		}
	}
	return runtime.ControlPlaneDone{Req: request, Resp: response}
}

func (c *ControlPlane) execDescribe(request runtime.RequestID, operation contract.DescribeTopics) runtime.IOResult {
	var response contract.DescribeTopicsResp
	for _, name := range sortedKeys(c.topics) {
		if len(operation.Topics) == 0 || slices.Contains(operation.Topics, name) {
			response.Topics = append(response.Topics, contract.TopicInfo{Name: name, Partitions: c.topics[name]})
		}
	}
	return runtime.ControlPlaneDone{Req: request, Resp: response}
}

func (c *ControlPlane) append(r contract.BatchRange) {
	if c.parts[r.TopicPartition.Topic] == nil {
		c.parts[r.TopicPartition.Topic] = map[int32][]contract.BatchRange{}
	}
	c.parts[r.TopicPartition.Topic][r.TopicPartition.Partition] = append(c.parts[r.TopicPartition.Topic][r.TopicPartition.Partition], r)
	c.topics[r.TopicPartition.Topic] = max(c.topics[r.TopicPartition.Topic], r.TopicPartition.Partition+1)
}

// StateHash folds committed segments (sorted by key, with their ranges) and
// topics (sorted by name).
func (c *ControlPlane) StateHash() uint64 {
	c.segPre = nextPrefix(c.segPre, len(c.segKeys), func(i int, h stateHasher) stateHasher {
		h.StringValue(c.segKeys[i])
		hashBatchRanges(&h, c.segs[c.segKeys[i]].ranges)
		return h
	})
	return c.tailHash(c.segPre[len(c.segKeys)])
}

// fullStateHash is StateHash without the segment prefix cache (for tests).
func (c *ControlPlane) fullStateHash() uint64 {
	h := newStateHasher()
	for _, k := range sortedKeys(c.segs) {
		h.StringValue(k)
		hashBatchRanges(&h, c.segs[k].ranges)
	}
	return c.tailHash(h)
}

// tailHash folds topics into h.
func (c *ControlPlane) tailHash(h stateHasher) uint64 {
	for _, t := range sortedKeys(c.topics) {
		h.StringValue(t)
		h.Uint64Value(uint64(c.topics[t]))
	}
	return uint64(h)
}
