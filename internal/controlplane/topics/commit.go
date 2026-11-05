// Package topics is the topics and segments context of the control plane: the
// topics and their partition counts, the segment commits that give out offsets,
// the idempotency window of the segment keys, and the lookups of runs. It keeps
// its state in the ordered key-value store under the keys T, E, R, K and S, and
// metadb lists its commands and reads in the topic schema. The functions run in
// a transaction the caller opens.
//
// Layout (all keys are prefix-free per kind):
//
//	T name                         topic -> partition count
//	E tp                           partition -> end offset (next offset)
//	R tp base(8, big endian)       one packed run per (partition, segment commit)
//	K segment-key                  idempotency record: slot, request digest, bases
//	S slot(8) segment-key          window index, so Trim can drop old K records
//
// where tp is uvarint(len topic), topic, partition (4, big endian). A run holds
// the contiguous batches one commit assigned to one partition, packed as a key
// table followed by varints per batch, about 6 B per batch.
//
// Idempotency window (decision 48): a K record is kept until Trim drops it,
// that is for as long as the log keeps the command. A replay inside the window
// gets the original ranges (rebuilt from the replayed request and the stored
// bases) or ErrConflict when the request digest differs. After Trim the key is
// forgotten and the same key commits again as a new segment.
//
// Nothing here knows about consumer groups. The groups context asks this one
// how many partitions a topic has, through PartitionCount.
package topics

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Known answers operation from the idempotency window when its key is already
// present. ok is false when the key is unknown (the caller must append).
func Known(transaction engine.Txn, operation contract.CommitSegment) (runtime.ControlPlaneDone, bool) {
	rec := transaction.Get(duplicateCheckKey(operation.SegmentKey))
	if rec == nil {
		return runtime.ControlPlaneDone{}, false
	}
	return replay(rec, checksum(operation.Batches), operation), true
}

// Lookup answers a Lookup from the stored runs.
func Lookup(transaction engine.Txn, operation contract.FindSegments) runtime.ControlPlaneDone {
	if operation.From < 0 {
		return runtime.ControlPlaneDone{Err: runtime.ErrInvalid}
	}
	return lookupTransaction(transaction, operation)
}

// Describe answers a DescribeTopics.
func Describe(transaction engine.Txn, operation contract.DescribeTopics) runtime.ControlPlaneDone {
	return describeTransaction(transaction, operation)
}

// Create creates name with at least partitions partitions (never shrinks).
// ErrInvalid for an empty name or partitions <= 0. With failIfExists, an
// existing topic gives ErrTopicExists and nothing changes. The check and the
// write are in one transaction, so the answer depends only on the state before
// this command.
func Create(transaction engine.Txn, name string, partitions int32, failIfExists bool) runtime.ErrCode {
	if name == "" || partitions <= 0 {
		return runtime.ErrInvalid
	}
	if failIfExists && transaction.Get(topicKey(name)) != nil {
		return runtime.ErrTopicExists
	}
	addPartitions(transaction, map[string]int32{}, name, partitions)
	return runtime.OK
}

// End is the next offset of topicPartition (0 if unknown).
func End(transaction engine.Txn, topicPartition contract.TopicPartition) contract.Offset {
	return contract.Offset(readVarint(transaction.Get(endKey(topicPartition))))
}

// PartitionCount is how many partitions topic has (0 when it does not exist).
func PartitionCount(transaction engine.Txn, topic string) int64 {
	return readVarint(transaction.Get(topicKey(topic)))
}

// InWindow reports whether key belongs to the idempotency window (the K and S
// keys). The state hash leaves these out, because Trim may drop them.
func InWindow(key []byte) bool { return key[0] == prefixKey || key[0] == prefixSlot }

// Trim forgets the idempotency records of commands at slots below slot (the
// log no longer holds them) and returns how many keys it dropped. Offsets and
// runs are untouched. A key forgotten here commits again as a new segment.
func Trim(transaction engine.Txn, slot int64) int {
	hi := binary.BigEndian.AppendUint64([]byte{prefixSlot}, uint64(slot))
	var keysToDrop [][]byte
	transaction.Ascend([]byte{prefixSlot}, hi, func(k, _ []byte) bool {
		keysToDrop = append(keysToDrop, bytes.Clone(k))
		return true
	})
	for _, k := range keysToDrop {
		transaction.Delete(duplicateCheckKey(string(k[9:])))
		transaction.Delete(k)
	}
	return len(keysToDrop)
}

// Window is the number of segment keys currently remembered.
func Window(transaction engine.Txn) int {
	var n int
	lo := []byte{prefixSlot}
	transaction.Ascend(lo, engine.PrefixEnd(lo), func(_, _ []byte) bool { n++; return true })
	return n
}

// Assigned reports the ranges operation was given if it is the first commit of its
// key at slot, rebuilt from the key's record. An indexer uses it to write the
// run rows of a slot after the fact.
func Assigned(transaction engine.Txn, slot int64, operation contract.CommitSegment) ([]contract.BatchRange, AssignState) {
	rec := transaction.Get(duplicateCheckKey(operation.SegmentKey))
	if rec == nil {
		return nil, OffsetOwnerUnknown
	}
	first, _ := binary.Uvarint(rec)
	d := replay(rec, checksum(operation.Batches), operation)
	if int64(first) != slot || d.Err != runtime.OK {
		return nil, OffsetOwnedElsewhere
	}
	return d.Resp.(contract.CommitSegmentResp).Assigned, OffsetOwnedHere
}

// AssignState says what Assigned knows about a command.
type AssignState int

// Assigned results.
const (
	// OffsetOwnerUnknown: no record of the key (invalid, or forgotten by Trim).
	OffsetOwnerUnknown AssignState = iota
	// OffsetOwnedElsewhere: the key committed at another slot, or with another
	// request: this command was a replay or a conflict and assigned nothing.
	OffsetOwnedElsewhere
	// OffsetOwnedHere: the command is the first commit of its key, at this slot.
	OffsetOwnedHere
)

func validBatch(b contract.PendingBatch) bool {
	return b.Records > 0 && b.TopicPartition.Topic != "" && b.TopicPartition.Partition >= 0
}

func validCommit(operation contract.CommitSegment) bool {
	return operation.SegmentKey != "" && len(operation.Batches) > 0 && slices.IndexFunc(operation.Batches, func(b contract.PendingBatch) bool { return !validBatch(b) }) < 0
}

// Commit applies a CommitSegment that sits at log slot slot. A key that is
// already known gets its original answer, or ErrConflict if the request differs.
func Commit(transaction engine.Txn, slot int64, operation contract.CommitSegment) runtime.ControlPlaneDone {
	dig := checksum(operation.Batches)
	if rec := transaction.Get(duplicateCheckKey(operation.SegmentKey)); rec != nil {
		return replay(rec, dig, operation)
	}
	if !validCommit(operation) {
		return runtime.ControlPlaneDone{Err: runtime.ErrInvalid}
	}
	return storeCommit(transaction, slot, operation, dig, assign(transaction, operation.Batches))
}

// replay answers a commit whose key is known: the original ranges, or
// ErrConflict if the request differs.
func replay(rec []byte, dig [checksumLength]byte, operation contract.CommitSegment) runtime.ControlPlaneDone {
	was, bases, err := parseDuplicateCheck(rec)
	if err != nil {
		panic(fmt.Errorf("topics: key %q: %w", operation.SegmentKey, err))
	}
	if was != dig {
		return runtime.ControlPlaneDone{Err: runtime.ErrConflict}
	}
	if len(bases) != len(operation.Batches) {
		panic(fmt.Errorf("topics: key %q: %w", operation.SegmentKey, errCorrupt))
	}
	rs := make([]contract.BatchRange, len(operation.Batches))
	for i, b := range operation.Batches {
		rs[i] = contract.BatchRange{TopicPartition: b.TopicPartition, Base: contract.Offset(bases[i]), Count: b.Records, Loc: b.Loc}
	}
	return runtime.ControlPlaneDone{Resp: contract.CommitSegmentResp{Assigned: rs}, Outcome: runtime.Applied}
}

func storeCommit(transaction engine.Txn, slot int64, operation contract.CommitSegment, dig [checksumLength]byte, assigned []contract.BatchRange) runtime.ControlPlaneDone {
	transaction.Set(duplicateCheckKey(operation.SegmentKey), duplicateCheckRecord(slot, dig, assigned))
	transaction.Set(slotKey(slot, operation.SegmentKey), []byte{1})
	return runtime.ControlPlaneDone{Resp: contract.CommitSegmentResp{Assigned: assigned}, Outcome: runtime.Applied}
}

// offsetPlan is the per-partition end and run built while handing out offsets.
type offsetPlan struct {
	order []contract.TopicPartition
	ends  map[contract.TopicPartition]int64
	runs  map[contract.TopicPartition][]contract.BatchRange
	seen  map[contract.TopicPartition]bool
}

func newOffsetPlan() *offsetPlan {
	return &offsetPlan{
		ends: map[contract.TopicPartition]int64{},
		runs: map[contract.TopicPartition][]contract.BatchRange{},
		seen: map[contract.TopicPartition]bool{},
	}
}

func (p *offsetPlan) add(b contract.PendingBatch, base int64) contract.BatchRange {
	if !p.seen[b.TopicPartition] {
		p.order = append(p.order, b.TopicPartition)
		p.seen[b.TopicPartition] = true
	}
	r := contract.BatchRange{TopicPartition: b.TopicPartition, Base: contract.Offset(base), Count: b.Records, Loc: b.Loc}
	if end := base + b.Records; end > p.ends[b.TopicPartition] {
		p.ends[b.TopicPartition] = end
	}
	p.runs[b.TopicPartition] = append(p.runs[b.TopicPartition], r)
	return r
}

func (p *offsetPlan) write(transaction engine.Txn) {
	writeAssign(transaction, p.order, p.ends, p.runs)
}

// assign hands out offsets: each batch's Base is its partition's end, in
// request order, the end then growing by Records. It writes the new ends, one
// packed run per partition and grown topics, and returns the ranges.
func assign(transaction engine.Txn, bs []contract.PendingBatch) []contract.BatchRange {
	p := newOffsetPlan()
	out := make([]contract.BatchRange, 0, len(bs))
	for _, b := range bs {
		base := p.ends[b.TopicPartition]
		if !p.seen[b.TopicPartition] {
			base = readVarint(transaction.Get(endKey(b.TopicPartition)))
		}
		out = append(out, p.add(b, base))
	}
	p.write(transaction)
	return out
}

func writeAssign(transaction engine.Txn, order []contract.TopicPartition, ends map[contract.TopicPartition]int64, runs map[contract.TopicPartition][]contract.BatchRange) {
	topics := map[string]int32{}
	for _, topicPartition := range order {
		transaction.Set(endKey(topicPartition), writeVarint(ends[topicPartition]))
		transaction.Set(batchRunKey(topicPartition, int64(runs[topicPartition][0].Base)), encodeBatchRun(runs[topicPartition]))
		addPartitions(transaction, topics, topicPartition.Topic, topicPartition.Partition+1)
	}
}

// addPartitions raises topic's partition count to at least n; seen caches the
// counts read in this transaction.
func addPartitions(transaction engine.Txn, seen map[string]int32, topic string, n int32) {
	cur, ok := seen[topic]
	if !ok {
		cur = int32(readVarint(transaction.Get(topicKey(topic))))
	}
	if n > cur {
		cur = n
		transaction.Set(topicKey(topic), writeVarint(int64(cur)))
	}
	seen[topic] = cur
}

func lookupTransaction(transaction engine.Txn, operation contract.FindSegments) runtime.ControlPlaneDone {
	response := contract.FindSegmentsResp{End: contract.Offset(readVarint(transaction.Get(endKey(operation.TopicPartition))))}
	lo := batchRunKey(operation.TopicPartition, 0)
	hi := engine.PrefixEnd(topicPartitionPrefix(prefixRun, operation.TopicPartition))
	if k, _ := transaction.Floor(lo, append(batchRunKey(operation.TopicPartition, int64(operation.From)), 0)); k != nil {
		lo = k // the run holding From starts at or before it
	}
	var size int64
	var bad error
	transaction.Ascend(lo, hi, func(k, v []byte) bool {
		rs, err := decodeBatchRun(operation.TopicPartition, batchRunBase(k), v)
		if err != nil {
			bad = err
			return false
		}
		for _, r := range rs {
			if r.Base+contract.Offset(r.Count) <= operation.From {
				continue
			}
			response.Ranges = append(response.Ranges, r)
			size += r.Loc.Len
			if operation.MaxBytes > 0 && size >= int64(operation.MaxBytes) {
				return false
			}
		}
		return true
	})
	if bad != nil {
		panic(fmt.Errorf("topics: %v: %w", operation.TopicPartition, bad))
	}
	return runtime.ControlPlaneDone{Resp: response}
}

func describeTransaction(transaction engine.Txn, operation contract.DescribeTopics) runtime.ControlPlaneDone {
	var response contract.DescribeTopicsResp
	lo := []byte{prefixTopic}
	transaction.Ascend(lo, engine.PrefixEnd(lo), func(k, v []byte) bool {
		name := string(bytes.Clone(k[1:]))
		if len(operation.Topics) == 0 || slices.Contains(operation.Topics, name) {
			response.Topics = append(response.Topics, contract.TopicInfo{Name: name, Partitions: int32(readVarint(v))})
		}
		return true
	})
	return runtime.ControlPlaneDone{Resp: response}
}
