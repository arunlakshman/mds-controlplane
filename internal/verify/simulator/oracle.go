package simulator

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/manifesterio/dst/check"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// oracle judges the answers that clients see. It has two parts.
//
// While the run goes on it keeps lower bounds taken from real time: what a
// client was told before it sent a read, the read must show (see snapshot and
// checkRead). These bounds need no order of the writes.
//
// After the run it replays the log, slot by slot, into the spec
// (the specModel). The log is the truth about the order of the writes,
// including the writes whose answer was lost. Then every recorded read must
// equal the spec at the prefix of the log that the node had applied, and every
// acked write must be in the log (see verify).
type oracle struct {
	s     *sim
	acks  []ackRec
	reads []readRec
	floor floors
}

type ackRec struct {
	op   runtime.ControlPlaneOperation
	done runtime.ControlPlaneDone
	at   int64
}

type readRec struct {
	op     runtime.ControlPlaneOperation
	done   runtime.ControlPlaneDone
	prefix runtime.LogPosition // slots applied by the node when it answered
	at     int64
	final  bool
}

// floors are the lower bounds from what clients have been told.
type floors struct {
	ends   check.Monotonic[contract.TopicPartition, contract.Offset]
	ranges map[contract.TopicPartition][]contract.BatchRange
	topics []contract.TopicInfo // sorted by name
	gens   check.Monotonic[string, int32]
}

// snapshot is a copy of the floors that matter to one read, taken when the read is sent.
type snapshot struct {
	end    contract.Offset
	ranges []contract.BatchRange
	topics []contract.TopicInfo
	gen    int32
}

func newOracle(s *sim) *oracle {
	return &oracle{s: s, floor: floors{
		ranges: map[contract.TopicPartition][]contract.BatchRange{},
	}}
}

func (f *floors) raiseTopic(name string, partitions int32) {
	i, found := slices.BinarySearchFunc(f.topics, name, func(t contract.TopicInfo, n string) int { return cmp.Compare(t.Name, n) })
	if found {
		f.topics[i].Partitions = max(f.topics[i].Partitions, partitions)
		return
	}
	f.topics = slices.Insert(f.topics, i, contract.TopicInfo{Name: name, Partitions: partitions})
}

// snapshotFor copies the floors that a read of op must respect.
func (o *oracle) snapshotFor(op runtime.ControlPlaneOperation) *snapshot {
	f := &o.floor
	switch q := op.(type) {
	case contract.FindSegments:
		return &snapshot{end: f.ends.Floor(q.TopicPartition), ranges: slices.Clone(f.ranges[q.TopicPartition])}
	case contract.DescribeTopics:
		snap := &snapshot{}
		for _, t := range f.topics {
			if len(q.Topics) == 0 || slices.Contains(q.Topics, t.Name) {
				snap.topics = append(snap.topics, t)
			}
		}
		return snap
	case contract.DescribeGroup:
		return &snapshot{gen: f.gens.Floor(q.Group)}
	}
	return nil
}

// record handles a final (not retried) answer of a client.
func (o *oracle) record(op *opState, d runtime.ControlPlaneDone, prefix runtime.LogPosition) {
	s := o.s
	if d.Err != runtime.OK {
		s.stats.Rejected++
		return
	}
	if op.write {
		s.stats.Acked++
		o.acks = append(o.acks, ackRec{op: op.op, done: d, at: int64(s.mono())})
		o.raiseFloors(op.op, d)
		s.clients.w.acked(op.op)
		return
	}
	s.stats.ReadsOK++
	o.reads = append(o.reads, readRec{op: op.op, done: d, prefix: prefix, at: int64(s.mono()), final: op.final})
	o.checkRead(op, d)
	o.observeRead(op.op, d)
}

// raiseFloors lifts the lower bounds after an acked write.
func (o *oracle) raiseFloors(op runtime.ControlPlaneOperation, d runtime.ControlPlaneDone) {
	f := &o.floor
	switch w := op.(type) {
	case contract.CommitSegment:
		resp, _ := d.Resp.(contract.CommitSegmentResp)
		for _, r := range resp.Assigned {
			tp := r.TopicPartition
			f.ends.Raise(tp, r.Base+contract.Offset(r.Count))
			f.ranges[tp] = append(f.ranges[tp], r)
			f.raiseTopic(tp.Topic, tp.Partition+1)
		}
	case contract.CreateTopic:
		f.raiseTopic(w.Name, w.Partitions)
	case contract.GroupInstall:
		f.gens.Raise(w.Group, w.Generation)
	}
}

// observeRead lifts the lower bounds after a read: a later read may not show less.
func (o *oracle) observeRead(op runtime.ControlPlaneOperation, d runtime.ControlPlaneDone) {
	f := &o.floor
	switch q := op.(type) {
	case contract.FindSegments:
		if r, ok := d.Resp.(contract.FindSegmentsResp); ok {
			f.ends.Raise(q.TopicPartition, r.End)
		}
	case contract.DescribeTopics:
		if r, ok := d.Resp.(contract.DescribeTopicsResp); ok {
			for _, t := range r.Topics {
				f.raiseTopic(t.Name, t.Partitions)
			}
		}
	}
}

// checkRead checks one read against the floors taken when it was sent: what was
// acked before the read was sent is visible, and offsets, segment lists and
// topics never go backwards.
func (o *oracle) checkRead(op *opState, d runtime.ControlPlaneDone) {
	snap := op.snap
	if snap == nil {
		return
	}
	s := o.s
	switch q := op.op.(type) {
	case contract.FindSegments:
		r, _ := d.Resp.(contract.FindSegmentsResp)
		if r.End < snap.end {
			s.violation("offsets never go backwards", "%v: end offset %d is below %d, which was known before the read was sent", q.TopicPartition, r.End, snap.end)
		}
		if q.MaxBytes == 0 {
			o.checkRanges(q, r, snap)
		}
	case contract.DescribeTopics:
		r, _ := d.Resp.(contract.DescribeTopicsResp)
		for _, want := range snap.topics {
			i := slices.IndexFunc(r.Topics, func(t contract.TopicInfo) bool { return t.Name == want.Name })
			if i < 0 || r.Topics[i].Partitions < want.Partitions {
				s.violation("acked write is visible", "topic %q with %d partitions was acked before the read, but describe shows %+v", want.Name, want.Partitions, r.Topics)
			}
		}
	case contract.DescribeGroup:
		r, _ := d.Resp.(contract.DescribeGroupResp)
		if snap.gen > 0 && (!r.Exists || r.Generation < snap.gen) {
			s.violation("acked write is visible", "group %q generation %d was acked before the read, but describe shows exists=%v generation=%d", q.Group, snap.gen, r.Exists, r.Generation)
		}
	}
}

// checkRanges checks an unbounded FindSegments: every range that was acked
// before the read and ends after From must be in the answer.
func (o *oracle) checkRanges(q contract.FindSegments, r contract.FindSegmentsResp, snap *snapshot) {
	for _, want := range snap.ranges {
		if want.Base+contract.Offset(want.Count) <= q.From {
			continue
		}
		if !slices.Contains(r.Ranges, want) {
			o.s.violation("segment lists never go backwards", "%v from %d: range %+v was acked before the read, but it is not in %+v", q.TopicPartition, q.From, want, r.Ranges)
			return
		}
	}
}

// sameAnswer compares two answers by error code and response. Nil and empty
// slices print the same with %+v, and that difference is not a bug.
func sameAnswer(a, b runtime.ControlPlaneDone) bool {
	return a.Err == b.Err && fmt.Sprintf("%+v", a.Resp) == fmt.Sprintf("%+v", b.Resp)
}

func describeAnswer(d runtime.ControlPlaneDone) string {
	return fmt.Sprintf("err=%v resp=%+v", d.Err, d.Resp)
}

// verify replays the log into the spec and judges everything that was recorded.
func (o *oracle) verify() {
	s := o.s
	end, _ := s.store.logEnd()
	s.check().NoHole(s.store)
	spec := newSpec()
	slices.SortStableFunc(o.reads, func(a, b readRec) int { return cmp.Compare(a.prefix, b.prefix) })
	next := 0
	judge := func(prefix runtime.LogPosition) {
		for next < len(o.reads) && o.reads[next].prefix == prefix {
			o.judgeRead(spec, o.reads[next], end)
			next++
		}
	}
	committed := map[string]runtime.ControlPlaneDone{}
	decode := func(entry []byte) ([]runtime.ControlPlaneOperation, error) {
		cmds, err := replica.EntryCommands(entry)
		var ops []runtime.ControlPlaneOperation
		for _, c := range cmds {
			if c.Op != nil {
				ops = append(ops, c.Op)
			}
		}
		return ops, err
	}
	apply := func(spec specModel, op runtime.ControlPlaneOperation) {
		d := spec.Apply(op)
		if cs, ok := op.(contract.CommitSegment); ok && d.Err == runtime.OK {
			if _, seen := committed[cs.SegmentKey]; !seen {
				committed[cs.SegmentKey] = d
			}
		}
	}
	if !check.Replay(s.check(), s.store, spec, decode, apply, func(prefix check.Slot) { judge(runtime.LogPosition(prefix)) }) {
		return
	}
	for ; next < len(o.reads); next++ {
		o.judgeRead(spec, o.reads[next], end)
	}
	o.judgeAcks(spec, committed)
}

// judgeRead checks that a read equals the spec after the prefix that the node had applied.
func (o *oracle) judgeRead(spec specModel, r readRec, end runtime.LogPosition) {
	s := o.s
	if !s.verdict(r.prefix <= end, "reads agree with the spec", "a node answered %T at slot %d, but the log has only %d slots", r.op, r.prefix, end) {
		return
	}
	if r.final {
		s.verdict(s.store.quietAfter(r.prefix), "live replicas converge", "a final read of %T was answered at slot %d, but the log has %d slots", r.op, r.prefix, end)
	}
	want := spec.Apply(r.op)
	s.verdict(sameAnswer(want, r.done), "reads agree with the spec", "at t=%dms after slot %d, %T: spec says %s, node says %s",
		r.at/int64(runtime.Millisecond), r.prefix, r.op, describeAnswer(want), describeAnswer(r.done))
}

// judgeAcks checks that every acked write is in the log, and gave the answer that the spec gives.
func (o *oracle) judgeAcks(spec specModel, committed map[string]runtime.ControlPlaneDone) {
	s := o.s
	for _, a := range o.acks {
		switch w := a.op.(type) {
		case contract.CommitSegment:
			want, in := committed[w.SegmentKey]
			if !s.verdict(in, "acked write is in the log", "commit %q was acked at t=%dms, but it is not in the log", w.SegmentKey, a.at/int64(runtime.Millisecond)) {
				continue
			}
			s.verdict(sameAnswer(want, a.done), "acked write has the spec answer", "commit %q: spec says %s, the ack says %s", w.SegmentKey, describeAnswer(want), describeAnswer(a.done))
		case contract.CreateTopic:
			got := spec.Apply(contract.DescribeTopics{Topics: []string{w.Name}})
			r, _ := got.Resp.(contract.DescribeTopicsResp)
			s.verdict(len(r.Topics) == 1 && r.Topics[0].Partitions >= w.Partitions, "acked write is in the log", "create topic %q (%d partitions) was acked, but the spec has %+v", w.Name, w.Partitions, r.Topics)
		case contract.GroupInstall:
			got := spec.Apply(contract.DescribeGroup{Group: w.Group})
			r, _ := got.Resp.(contract.DescribeGroupResp)
			s.verdict(r.Exists && r.Generation >= w.Generation, "acked write is in the log", "install of group %q generation %d was acked, but the spec has exists=%v generation=%d", w.Group, w.Generation, r.Exists, r.Generation)
		}
	}
}
