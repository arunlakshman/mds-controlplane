package controlplanetest

import (
	"bytes"
	"fmt"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// The alphabet of the conformance tests: the operations that rapid draws. Each
// action draws its own arguments, sends the operation to the model and to the
// system under test, and compares the answers (see world.do in conform_test.go).
//
// The actions are in a list from simple to complex: reads, then commit and
// create topic, then group operations, then background operations. Rapid draws
// the index of the action, and its shrinker moves toward index 0, so a failure
// is shrunk to the simplest actions that still show it.
//
// Names, keys and numbers come from small sets, so that the operations hit the
// same topic, key or group again and again. Without that, a replay, a conflict
// or a group with members would almost never happen. Besides that, only these
// choices lean on purpose (each is a draw of a small number that shrinks to 0,
// the plain case):
//   - about 1 name in 10 is empty, to reach the ErrInvalid rules;
//   - commit: a replay of an earlier commit, or an earlier key with other
//     records (a conflict), is one draw out of three;
//   - group install and assign use the generation and the leader that the model
//     says is current, 3 times in 4, because a random generation is almost
//     always ErrIllegalGeneration and the group would never get a member.
// TestAlphabetReachesTheRules checks the evidence: it counts the answers and
// fails when a rule was not reached.

type action struct {
	name string
	run  func(w *world)
}

// options choose which actions are in the alphabet.
type options struct {
	groups  bool // group operations (the gRPC API has no install or assign)
	retry   bool // only operations that are safe to send again after an error
	restart bool // Checkpoint and Reboot (the system is restartable)
	dirty   bool // DirtyReboot (the system is restartable)
	faults  bool // Inject (the system is faulty)
	trim    bool // the log is trimmed, so a dirty reboot may not drop checkpoints
}

var (
	topicNames  = []string{"a", "b", "c"}
	groupNames  = []string{"g1", "g2"}
	memberNames = []string{"m1", "m2", "m3"}
	segmentKeys = []string{"k0", "k1", "k2", "k3"}
)

// pick draws one of values, or "" about 1 time in 10 (see above).
func pick(t *rapid.T, label string, values []string) string {
	i := rapid.IntRange(0, 3*len(values)).Draw(t, label)
	if i == 3*len(values) {
		return ""
	}
	return values[i/3]
}

// rare is true about 1 time in 20.
func rare(t *rapid.T, label string) bool { return rapid.IntRange(0, 19).Draw(t, label) == 0 }

func drawTP(t *rapid.T) contract.TopicPartition {
	return contract.NewTopicPartition(pick(t, "topic", topicNames), int32(rapid.IntRange(0, 2).Draw(t, "partition")))
}

func drawBatch(t *rapid.T) contract.PendingBatch {
	records := int64(rapid.IntRange(1, 4).Draw(t, "records"))
	if rare(t, "no records") {
		records = 0
	}
	loc := runtime.SegmentRef{Key: "seg", Off: int64(rapid.IntRange(0, 50).Draw(t, "off")), Len: int64(rapid.IntRange(1, 12).Draw(t, "len"))}
	return contract.PendingBatch{TopicPartition: drawTP(t), Records: records, Loc: loc}
}

// alphabet returns the actions in the order of their complexity.
func alphabet(o options) []action {
	list := []action{
		{"findSegments", func(w *world) {
			from := int64(rapid.IntRange(0, 12).Draw(w.t, "from"))
			if rare(w.t, "negative from") {
				from = -1
			}
			op := contract.FindSegments{TopicPartition: drawTP(w.t), From: contract.Offset(from), MaxBytes: int32(rapid.IntRange(0, 25).Draw(w.t, "maxBytes"))}
			w.do("find", op)
			w.edgeFinds(op)
		}},
		{"describeTopics", func(w *world) {
			names := rapid.SliceOfN(rapid.SampledFrom(append(topicNames, "zz")), 0, 2).Draw(w.t, "names")
			w.do("describeTopics", contract.DescribeTopics{Topics: names})
		}},
	}
	if o.groups {
		list = append(list, groupReads()...)
	}
	list = append(list,
		action{"commit", func(w *world) { w.commit() }},
		action{"createTopic", func(w *world) {
			op := contract.CreateTopic{Name: pick(w.t, "name", topicNames), Partitions: int32(rapid.IntRange(0, 4).Draw(w.t, "partitions"))}
			if !o.retry {
				op.FailIfExists = rapid.Bool().Draw(w.t, "failIfExists")
			}
			w.do("createTopic", op)
		}},
	)
	if o.groups {
		list = append(list, groupWrites(o)...)
	}
	if o.restart || o.dirty || o.faults {
		list = append(list, backgroundActions(o)...)
	}
	return list
}

func (w *world) commit() {
	mode := rapid.IntRange(0, 2).Draw(w.t, "commit mode") // 0 fresh, 1 replay, 2 conflict
	if mode > 0 && len(w.commits) > 0 {
		old := w.commits[rapid.IntRange(0, len(w.commits)-1).Draw(w.t, "which commit")]
		if mode == 1 || len(old.Batches) == 0 {
			w.do("commit.replay", old)
			return
		}
		other := contract.CommitSegment{SegmentKey: old.SegmentKey, Batches: append([]contract.PendingBatch{}, old.Batches...)}
		other.Batches[0].Records++
		w.do("commit.conflict", other)
		return
	}
	op := contract.CommitSegment{SegmentKey: pick(w.t, "key", segmentKeys), Batches: rapid.SliceOfN(rapid.Custom(drawBatch), 0, 4).Draw(w.t, "batches")}
	w.commits = append(w.commits, op)
	w.do("commit", op)
}

// edgeFinds asks the same question again with MaxBytes just below, at and just
// above each running total of Loc.Len in the answer, because the stop rule
// (the sum reaches MaxBytes) is exactly on these values.
func (w *world) edgeFinds(op contract.FindSegments) {
	op.MaxBytes = 0
	answer, ok := w.model.Apply(op).Resp.(contract.FindSegmentsResp)
	if !ok {
		return
	}
	var sum int64
	for _, r := range answer.Ranges {
		sum += r.Loc.Len
		for _, edge := range []int64{sum - 1, sum, sum + 1} {
			if edge > 0 {
				op.MaxBytes = int32(edge)
				w.do("find.edge", op)
			}
		}
	}
}

func groupReads() []action {
	return []action{
		{"offsetFetch", func(w *world) {
			ps := rapid.SliceOfN(rapid.Custom(drawTP), 0, 3).Draw(w.t, "partitions")
			w.do("offsetFetch", contract.OffsetFetch{Group: pick(w.t, "group", groupNames), Partitions: ps})
		}},
		{"describeGroup", func(w *world) { w.do("describeGroup", contract.DescribeGroup{Group: pick(w.t, "group", groupNames)}) }},
		{"listGroups", func(w *world) { w.do("listGroups", contract.ListGroups{}) }},
	}
}

// current is what the model says about a group. The generators use it to build
// operations that are valid most of the time.
func (w *world) current(group string) contract.DescribeGroupResp {
	return w.model.Apply(contract.DescribeGroup{Group: group}).Resp.(contract.DescribeGroupResp)
}

func groupWrites(o options) []action {
	return []action{
		{"groupInstall", func(w *world) {
			group := pick(w.t, "group", groupNames)
			gen := w.current(group).Generation
			switch n := rapid.IntRange(0, 5).Draw(w.t, "generation"); {
			case n <= 3:
				gen++
			case n == 5:
				gen += 2
			}
			ids := rapid.SliceOfNDistinct(rapid.SampledFrom(memberNames), 0, 3, func(s string) string { return s }).Draw(w.t, "members")
			op := contract.GroupInstall{Group: group, ProtocolType: "consumer", Protocol: "range", Generation: gen, TimeMs: int64(rapid.IntRange(0, 1000).Draw(w.t, "time"))}
			for _, id := range ids {
				meta := rapid.SliceOfN(rapid.Byte(), 0, 3).Draw(w.t, "metadata")
				op.Members = append(op.Members, contract.GroupMember{MemberID: id, ClientID: "c-" + id, SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000, Metadata: meta})
			}
			if len(ids) > 0 {
				op.Leader = ids[rapid.IntRange(0, len(ids)-1).Draw(w.t, "leader")]
			}
			if rare(w.t, "other leader") {
				op.Leader = "m9"
			}
			w.do("groupInstall", op)
		}},
		{"groupAssign", func(w *world) {
			group := pick(w.t, "group", groupNames)
			cur := w.current(group)
			op := contract.GroupAssign{Group: group, Generation: cur.Generation, Leader: cur.Leader}
			if rapid.IntRange(0, 3).Draw(w.t, "stale") == 3 {
				op.Generation--
			}
			for _, m := range cur.Members {
				if rapid.Bool().Draw(w.t, "assigned") {
					op.Assignments = append(op.Assignments, contract.GroupAssignment{Member: m.MemberID, Bytes: []byte("a-" + m.MemberID)})
				}
			}
			if rare(w.t, "unknown member") {
				op.Assignments = append(op.Assignments, contract.GroupAssignment{Member: "zz", Bytes: []byte("z")})
			}
			w.do("groupAssign", op)
		}},
		{"groupRemove", func(w *world) {
			ids := rapid.SliceOfNDistinct(rapid.SampledFrom(memberNames), 0, 2, func(s string) string { return s }).Draw(w.t, "members")
			w.do("groupRemove", contract.GroupRemove{Group: pick(w.t, "group", groupNames), Members: ids, Reason: "leave", TimeMs: 1})
		}},
		{"offsetCommit", func(w *world) {
			group := pick(w.t, "group", groupNames)
			cur := w.current(group)
			op := contract.OffsetCommit{Group: group, Generation: -1, TimeMs: 7}
			switch n := rapid.IntRange(0, 3).Draw(w.t, "how"); {
			case n == 0 && len(cur.Members) > 0: // a member of the current generation
				op.Generation, op.Member = cur.Generation, cur.Members[0].MemberID
			case n == 2: // the next generation, which is not current
				op.Generation, op.Member = cur.Generation+1, "m1"
			case n == 3: // a manual commit with a member name
				op.Member = "m1"
			}
			for range rapid.IntRange(0, 3).Draw(w.t, "how many") {
				op.Offsets = append(op.Offsets, contract.PartitionOffset{TopicPartition: drawTP(w.t),
					Offset: int64(rapid.IntRange(0, 50).Draw(w.t, "offset")), LeaderEpoch: int32(rapid.IntRange(0, 3).Draw(w.t, "epoch")), Metadata: pick(w.t, "meta", []string{"x"})})
			}
			if rare(w.t, "negative offset") && len(op.Offsets) > 0 {
				op.Offsets[0].Offset = -1
			}
			w.do("offsetCommit", op)
		}},
		{"groupAssignTooLarge", func(w *world) {
			big := bytes.Repeat([]byte("x"), 300<<10)
			w.do("groupAssign.tooLarge", contract.GroupAssign{Group: "g1", Generation: 1, Leader: "m1", Assignments: []contract.GroupAssignment{{Member: "m1", Bytes: big}}})
		}},
	}
}

// backgroundActions are the background operations. The model does nothing for
// them: a restart must not change any answer.
func backgroundActions(o options) []action {
	list := []action{
		{"checkpoint", func(w *world) { w.sys.(restartable).Checkpoint() }},
		{"reboot", func(w *world) {
			w.sys.(restartable).Reboot()
			w.afterRestart("forward progress")
		}},
	}
	if o.dirty {
		list = append(list, action{"dirtyReboot", func(w *world) {
			keep := rapid.IntRange(0, 2).Draw(w.t, "checkpoints kept") // 0 all, 1 all but the newest, 2 none
			if o.trim {
				keep = 0 // the log is cut behind the checkpoints, so they are not allowed to go
			}
			w.sys.(restartable).DirtyReboot(keep)
			w.afterRestart("persistence")
		}})
	}
	if o.faults {
		list = append(list, action{"injectFailure", func(w *world) {
			w.sys.(faulty).Inject(rapid.IntRange(0, 3).Draw(w.t, "failure"))
			w.hasFailed = true
		}})
	}
	return list
}

// afterRestart checks the property after a restart. After a clean reboot it is
// forward progress, after a dirty one it is persistence. Both say the same
// thing in a test that waits for each answer: every write that was answered
// OK before the restart is there after it, and no other write is. So the whole
// state must equal the model, and each commit that was sent before must still
// be a replay with the same ranges.
func (w *world) afterRestart(property string) {
	w.property = property
	defer func() { w.property = "" }()
	w.do("sweep.topics", contract.DescribeTopics{})
	for _, topic := range topicNames {
		for p := range int32(3) {
			w.do("sweep.find", contract.FindSegments{TopicPartition: contract.NewTopicPartition(topic, p)})
		}
	}
	for _, c := range w.commits {
		w.do("sweep.replay", c)
	}
	if w.groups {
		w.do("sweep.groups", contract.ListGroups{})
		for _, g := range groupNames {
			w.do("sweep.describeGroup", contract.DescribeGroup{Group: g})
			w.do("sweep.offsets", contract.OffsetFetch{Group: g})
		}
	}
}

// describe prints an operation for a failure message.
func describe(op runtime.ControlPlaneOperation) string { return fmt.Sprintf("%T%+v", op, op) }
