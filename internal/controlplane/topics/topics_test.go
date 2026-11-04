package topics_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/controlplanetest/kvtest"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/kv"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadata"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/verify/speccheck/checker"
	"github.com/manifesterio/mds-controlplane/internal/verify/speccheck/checker/generator"
	"github.com/manifesterio/mds-controlplane/internal/verify/speccheck/checker/injectedbug"
	"github.com/manifesterio/mds-controlplane/internal/verify/speccheck/checker/observation"
	"github.com/manifesterio/mds-controlplane/internal/verify/speccheck/checker/runsettings"
	"github.com/manifesterio/mds-controlplane/internal/verify/speccheck/checker/suites"
)

// metadataSubject runs the Store directly as a suite subject. The applier
// hash is not the hash of the spec, so StateHash says no.
type metadataSubject struct{ a *metadata.Store }

func (s metadataSubject) Do(operation generator.ControlPlaneTestStep) (observation.ControlPlaneResult, error) {
	switch {
	case operation.CreateTopic != nil:
		return observation.ControlPlaneResult{Code: s.a.CreateTopic(operation.CreateTopic.Name, operation.CreateTopic.Partitions)}, nil
	case operation.Call != nil:
		done, ok := s.a.Exec(*operation.Call).(runtime.ControlPlaneDone)
		if !ok {
			return observation.ControlPlaneResult{}, errors.New("the applier did not return a ControlPlaneDone")
		}
		return observation.FromControlPlaneDone(done), nil
	}
	return observation.ControlPlaneResult{}, errors.New("the operation has neither CreateTopic nor Call")
}

func (metadataSubject) StateHash() (uint64, bool) { return 0, false }

func newMetadataSubject(t testing.TB, mk func(testing.TB) kv.Store) suites.ControlPlaneSubject {
	a := metadata.New(mk(t))
	t.Cleanup(func() { _ = a.Close() })
	return metadataSubject{a}
}

// runSuite runs the shared control plane suite on the applier for every
// profile. Pebble is slower, so it passes a smaller default number of seeds.
func runSuite(t *testing.T, mk func(testing.TB) kv.Store, defaultSeeds int) {
	for _, profile := range suites.AllControlPlaneProfiles() {
		t.Run(profile.Name, func(t *testing.T) {
			suites.ControlPlane(t, profile, func() suites.ControlPlaneSubject { return newMetadataSubject(t, mk) },
				runsettings.Options(t, defaultSeeds))
		})
	}
}

func TestLockstepMem(t *testing.T) { runSuite(t, kvtest.Backends["mem"], 20) }

func TestLockstepPebble(t *testing.T) { runSuite(t, kvtest.Backends["pebble"], 3) }

// TestLookupMaxBytesEdges checks what the shared suite does not: a Lookup with
// MaxBytes at, just below and just above every running total of Len in an
// answer. The stop rule is a >= on exactly these values, and the suite only
// uses fixed MaxBytes values.
func TestLookupMaxBytesEdges(t *testing.T) {
	for name, mk := range kvtest.Backends {
		seeds := 40
		if name == "pebble" {
			seeds = 3
		}
		t.Run(name, func(t *testing.T) {
			profile := generator.LookupCuts()
			ops := generator.ControlPlaneOps(profile)
			for seed := uint64(1); seed <= uint64(seeds); seed++ {
				r := checker.NewRand(seed)
				a, spec := metadata.New(mk(t)), suites.NewSpecControlPlaneSubject()
				subject := metadataSubject{a}
				for i := range 120 {
					operation := ops(r, i)
					expected, _ := spec.Do(operation)
					actual, _ := subject.Do(operation)
					if diff := observation.DiffControlPlane(expected, actual); diff != "" {
						t.Fatalf("Seed %d, operation %d (%s): %s", seed, i, operation.Describe(), diff)
					}
					if operation.Call != nil {
						if lk, ok := operation.Call.Op.(contract.FindSegments); ok {
							edgeLookups(t, subject, spec, lk, expected)
						}
					}
				}
				_ = a.Close()
			}
		})
	}
}

func TestTrimForgetsKeysNotOffsets(t *testing.T) {
	a := metadata.New(kv.NewMemory())
	mustCommit(t, a, 0, commit("old", pb("a", 0, 5)))
	mustCommit(t, a, 5, commit("new", pb("a", 0, 5)))
	h := a.StateHash()
	if n, err := a.Trim(5); err != nil || n != 1 || a.Window() != 1 {
		t.Fatalf("Trim dropped %d err %v window %d", n, err, a.Window())
	}
	if a.StateHash() != h || a.End(contract.NewTopicPartition("a", 0)) != 10 {
		t.Fatal("trim changed offsets or runs")
	}
	// "new" is still in the window: replay answers the original ranges.
	if d := mustCommit(t, a, 6, commit("new", pb("a", 0, 5))); d.Resp.(contract.CommitSegmentResp).Assigned[0].Base != 5 {
		t.Fatalf("window replay: %+v", d)
	}
	// "old" left the window: the same key commits again as a NEW segment (decision 48).
	d := mustCommit(t, a, 7, commit("old", pb("a", 0, 5)))
	if d.Err != runtime.OK || d.Resp.(contract.CommitSegmentResp).Assigned[0].Base != 10 {
		t.Fatalf("after trim the key must commit again at 10: %+v", d)
	}
}

func TestInvalidAndUnknown(t *testing.T) {
	a := metadata.New(kv.NewMemory())
	if a.CreateTopic("", 1) != runtime.ErrInvalid || a.CreateTopic("t", 0) != runtime.ErrInvalid {
		t.Fatal("CreateTopic must reject empty name and non-positive partitions")
	}
	if d := a.Exec(runtime.ControlPlaneCall{Op: contract.FindSegments{From: -1}}).(runtime.ControlPlaneDone); d.Err != runtime.ErrInvalid {
		t.Fatalf("Lookup From<0: %+v", d)
	}
	if d := a.Exec(runtime.ControlPlaneCall{Op: nil}).(runtime.ControlPlaneDone); d.Err != runtime.ErrInvalid {
		t.Fatalf("unknown op: %+v", d)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Exec of a non-ControlPlaneCall must panic")
		}
	}()
	a.Exec(runtime.ObjectPut{})
}

// edgeLookups asks lk again with MaxBytes at, just below and just above every
// running total of Len in the expected answer.
func edgeLookups(t *testing.T, subject, spec suites.ControlPlaneSubject, lk contract.FindSegments, expected observation.ControlPlaneResult) {
	t.Helper()
	var sum int64
	for _, r := range expected.Ranges {
		sum += r.Loc.Len
		for e := int64(-1); e <= 1; e++ {
			if sum+e <= 0 {
				continue
			}
			q := generator.NewCall(runtime.ControlPlaneCall{Op: contract.FindSegments{TopicPartition: lk.TopicPartition, From: lk.From, MaxBytes: int32(sum + e)}})
			want, _ := spec.Do(q)
			got, _ := subject.Do(q)
			if diff := observation.DiffControlPlane(want, got); diff != "" {
				t.Fatalf("For %s: %s", q.Describe(), diff)
			}
		}
	}
}

// TestInjectedBugsCaughtMetadataMem proves that the suite fails when the applier
// has a known bug. The applier hash is not the hash of the spec, so the helper
// skips the one bug that only a state hash can show.
func TestInjectedBugsCaughtMetadataMem(t *testing.T) {
	newCorrect := func() suites.ControlPlaneSubject { return newMetadataSubject(t, kvtest.Backends["mem"]) }
	injectedbug.RequireAllControlPlaneBugsDetected(t, newCorrect, 100)
}

// TestCreateTopicExclusive runs the create steps in log order on both stores.
// The exclusive create checks and writes in one step, so a second exclusive
// create of a name that exists is ErrTopicExists and changes nothing.
func TestCreateTopicExclusive(t *testing.T) {
	steps := []struct {
		name      string
		operation contract.CreateTopic
		code      runtime.ErrCode
		parts     int32 // partitions of the topic after the step
	}{
		{"exclusive create of a new topic", contract.CreateTopic{Name: "t", Partitions: 2, FailIfExists: true}, runtime.OK, 2},
		{"exclusive create again", contract.CreateTopic{Name: "t", Partitions: 5, FailIfExists: true}, runtime.ErrTopicExists, 2},
		{"plain create again is still at least", contract.CreateTopic{Name: "t", Partitions: 4}, runtime.OK, 4},
		{"plain create never shrinks", contract.CreateTopic{Name: "t", Partitions: 1}, runtime.OK, 4},
		{"exclusive create after a plain one", contract.CreateTopic{Name: "t", Partitions: 1, FailIfExists: true}, runtime.ErrTopicExists, 4},
		{"exclusive create of another new topic", contract.CreateTopic{Name: "u", Partitions: 1, FailIfExists: true}, runtime.OK, 1},
		{"exclusive create, empty name", contract.CreateTopic{Name: "", Partitions: 1, FailIfExists: true}, runtime.ErrInvalid, 0},
		{"exclusive create, zero partitions", contract.CreateTopic{Name: "z", Partitions: 0, FailIfExists: true}, runtime.ErrInvalid, 0},
	}
	for name, mk := range kvtest.Backends {
		t.Run(name, func(t *testing.T) {
			a := metadata.New(mk(t))
			t.Cleanup(func() { _ = a.Close() })
			for i, s := range steps {
				before := a.StateHash()
				d := a.Exec(runtime.ControlPlaneCall{Req: runtime.RequestID(i), Op: s.operation}).(runtime.ControlPlaneDone)
				if d.Err != s.code {
					t.Fatalf("%s: code %v, want %v", s.name, d.Err, s.code)
				}
				if s.code != runtime.OK && a.StateHash() != before {
					t.Fatalf("%s: the state changed on a failed create", s.name)
				}
				if s.code == runtime.OK && d.Outcome != runtime.Applied {
					t.Fatalf("%s: outcome %v, want Applied", s.name, d.Outcome)
				}
				if s.parts == 0 {
					continue
				}
				got, err := a.Describe(contract.DescribeTopics{Topics: []string{s.operation.Name}})
				if err != nil {
					t.Fatal(err)
				}
				if topics := got.Resp.(contract.DescribeTopicsResp).Topics; len(topics) != 1 || topics[0].Partitions != s.parts {
					t.Fatalf("%s: topics %+v, want %d partitions", s.name, topics, s.parts)
				}
			}
		})
	}
}

func TestAssignedStates(t *testing.T) {
	a := metadata.New(kv.NewMemory())
	operation := commit("k1", pb("a", 0, 3), pb("a", 1, 2))
	first := mustCommit(t, a, 5, operation)
	want := first.Resp.(contract.CommitSegmentResp).Assigned

	rs, st, err := a.Assigned(5, operation)
	if err != nil || st != metadata.OffsetOwnedHere || !reflect.DeepEqual(rs, want) {
		t.Fatalf("here: %v %v %+v", err, st, rs)
	}
	if _, st, _ = a.Assigned(9, operation); st != metadata.OffsetOwnedElsewhere { // a replay at a later slot
		t.Fatalf("other slot: %v", st)
	}
	if _, st, _ = a.Assigned(5, commit("k1", pb("a", 0, 4))); st != metadata.OffsetOwnedElsewhere { // a conflict
		t.Fatalf("conflict: %v", st)
	}
	if _, st, _ = a.Assigned(5, commit("nope", pb("a", 0, 1))); st != metadata.OffsetOwnerUnknown {
		t.Fatalf("unknown: %v", st)
	}
	if _, err = a.Trim(6); err != nil {
		t.Fatal(err)
	}
	if _, st, _ = a.Assigned(5, operation); st != metadata.OffsetOwnerUnknown { // forgotten by the window
		t.Fatalf("trimmed: %v", st)
	}
}

func TestCommitWithWriterBases(t *testing.T) {
	a := metadata.NewMem()
	operation := contract.CommitSegment{
		SegmentKey: "seg/1",
		Batches: []contract.PendingBatch{
			{TopicPartition: contract.NewTopicPartition("t", 0), Records: 3, Loc: runtime.SegmentRef{Key: "o", Off: 0, Len: 1}},
		},
	}
	d := a.Apply(1, metadata.Entry{
		ID: "w/1", Kind: metadata.KindCommit, Op: operation,
		WriterBases: []contract.Offset{5},
	})
	if d.Err != runtime.OK || d.Outcome != runtime.Applied {
		t.Fatal(d)
	}
	response := runtime.ResponseAs[contract.CommitSegmentResp](d)
	if len(response.Assigned) != 1 || response.Assigned[0].Base != 5 || response.Assigned[0].Count != 3 {
		t.Fatalf("%+v", response)
	}
	if a.End(contract.NewTopicPartition("t", 0)) != 8 {
		t.Fatal(a.End(contract.NewTopicPartition("t", 0)))
	}
}

func commit(key string, bs ...contract.PendingBatch) contract.CommitSegment {
	return contract.CommitSegment{SegmentKey: key, Batches: bs}
}

func pb(topic string, p int32, n int64) contract.PendingBatch {
	return contract.PendingBatch{TopicPartition: contract.NewTopicPartition(topic, p), Records: n,
		Loc: runtime.SegmentRef{Key: topic, Off: n * 10, Len: n}}
}

func mustCommit(t *testing.T, a *metadata.Store, slot int64, operation contract.CommitSegment) runtime.ControlPlaneDone {
	t.Helper()
	d, err := a.Commit(slot, operation)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
