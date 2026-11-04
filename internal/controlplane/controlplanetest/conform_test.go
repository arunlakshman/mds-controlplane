package controlplanetest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/controlplanetest/memorystore"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/controlplanetest/metadatastore"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/kv"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/kv/pebblestore"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadata"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/spec"
)

// Conformance: one property-based test (rapid) for each system. It draws a list
// of actions (alphabet_test.go), sends every operation to the model
// (internal/spec) and to the system, and compares the answers. After each step
// it checks the invariants. How to run more checks or one failure again is in
// internal/spec/README.md.

// world is the state of one check: the model, the system and what the checks
// need to remember.
type world struct {
	t     *rapid.T
	model *spec.ControlPlane
	sys   runtime.OperationRunner
	steps int

	commits   []contract.CommitSegment // every commit sent, to replay them
	hasFailed bool                     // a failure was injected (see ask)
	property  string                   // set while a restart is checked
	groups    bool
	seen      map[string]int // answers by action and code, for the evidence
}

// callTimeout bounds one call. A call that takes longer is a failure.
const callTimeout = 3 * time.Second

func (w *world) call(op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	ctx, cancel := context.WithTimeout(w.t.Context(), callTimeout)
	defer cancel()
	return w.sys.Exec(ctx, op)
}

// transient is an answer that says "try again": the system was not able to
// answer, and it did not say anything wrong.
func transient(d runtime.ControlPlaneDone, err error) bool {
	return err != nil || d.Err == runtime.ErrUnavailable || d.Err == runtime.ErrTimeout || d.Err == runtime.ErrIO
}

// ask sends op to the system. This is where the "has failed" flag works, as in
// the paper: once a failure was injected, an operation may answer with an error.
// The system is then asked again until it answers for real, and that answer is
// compared with the model. An error is allowed, a wrong answer never is. (The
// paper also allows "no data" after a failure. Our log does not lose data, so
// this check is stricter and does not allow it.) This is why a test with
// failures uses only operations that are safe to send again.
func (w *world) ask(op runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
	d, err := w.call(op)
	for try := 0; transient(d, err) && w.hasFailed && try < 300; try++ {
		if f, ok := w.sys.(faulty); ok {
			f.Inject(-1) // stop the failures, so the retry can succeed
		}
		time.Sleep(10 * time.Millisecond)
		d, err = w.call(op)
	}
	if err != nil {
		w.t.Fatalf("step %d: %s: the system returned an error: %v", w.steps, describe(op), err)
	}
	return d
}

// do sends one operation to the model and to the system and compares them.
func (w *world) do(label string, op runtime.ControlPlaneOperation) {
	w.t.Helper()
	want := w.model.Apply(op)
	got := w.ask(op)
	w.seen[label+"/"+want.Err.String()]++
	if !sameAnswer(got, want) {
		w.t.Fatalf("step %d%s: %s\n  model: %s\n  system: %s", w.steps, w.propertyText(), describe(op), show(want), show(got))
	}
	if msg := badAnswer(got); msg != "" {
		w.t.Fatalf("step %d: %s\n  invariant broken: %s\n  system: %s", w.steps, describe(op), msg, show(got))
	}
}

func (w *world) propertyText() string {
	if w.property == "" {
		return ""
	}
	return " (" + w.property + " after a restart)"
}

func show(d runtime.ControlPlaneDone) string { return fmt.Sprintf("err=%v resp=%+v", d.Err, d.Resp) }

// sameAnswer compares the code and the response. A nil slice and an empty slice
// print the same, and that difference is not a bug.
func sameAnswer(a, b runtime.ControlPlaneDone) bool {
	return a.Err == b.Err && fmt.Sprintf("%+v", a.Resp) == fmt.Sprintf("%+v", b.Resp)
}

// badAnswer checks the invariants of one answer, alone, without the model:
// the runs of a partition follow each other without a gap, the end offset is
// the end of the last run, and topics are sorted without repeats.
func badAnswer(d runtime.ControlPlaneDone) string {
	switch r := d.Resp.(type) {
	case contract.FindSegmentsResp:
		for i, x := range r.Ranges {
			if x.Count <= 0 || (i > 0 && x.Base != r.Ranges[i-1].Base+contract.Offset(r.Ranges[i-1].Count)) {
				return fmt.Sprintf("run %d is not next to the run before it: %+v", i, r.Ranges)
			}
		}
		if n := len(r.Ranges); n > 0 && r.End < r.Ranges[n-1].Base+contract.Offset(r.Ranges[n-1].Count) {
			return fmt.Sprintf("end offset %d is before the end of the last run", r.End)
		}
	case contract.DescribeTopicsResp:
		if !slices.IsSortedFunc(r.Topics, func(a, b contract.TopicInfo) int { return compareString(a.Name, b.Name) }) {
			return fmt.Sprintf("topics are not sorted: %+v", r.Topics)
		}
	}
	return ""
}

func compareString(a, b string) int {
	if a == b {
		return 0
	}
	if a < b {
		return -1
	}
	return 1
}

// Optional abilities of a system. A system that has them gets more actions.
type restartable interface {
	Reboot()
	DirtyReboot(keep int)
	Checkpoint()
}

type faulty interface {
	// Inject makes the next calls of one kind to the storage fail. -1 stops it.
	Inject(kind int)
}

// check runs the conformance property on the systems that newSystem makes. It
// returns the number of answers by action and code, over all checks.
func check(t rapid.TB, o options, newSystem func(*rapid.T) runtime.OperationRunner) map[string]int {
	seen := map[string]int{}
	rapid.Check(t, func(rt *rapid.T) {
		w := &world{t: rt, model: spec.New(), sys: newSystem(rt), groups: o.groups, seen: map[string]int{}}
		actions := alphabet(o)
		rt.Repeat(map[string]func(*rapid.T){"step": func(rt *rapid.T) {
			a := actions[rapid.IntRange(0, len(actions)-1).Draw(rt, "action")]
			rt.Logf("step %d: %s", w.steps, a.name)
			w.seen["action "+a.name]++
			a.run(w)
			w.steps++
		}})
		for k, v := range w.seen {
			seen[k] += v
		}
	})
	return seen
}

// userFlags are the flags that the person who runs the tests set. It is read on
// the first call, before setRapid changes anything (a flag.Set counts as set).
var userFlags = sync.OnceValue(func() map[string]bool {
	asked := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { asked[f.Name] = true })
	return asked
})

// setRapid makes slow tests run fewer checks than rapid's default of 100 and
// fewer steps than its default of 30, unless the person who runs them asked
// for numbers with -rapid.checks and -rapid.steps.
func setRapid(t *testing.T, checks, steps int) {
	t.Helper()
	asked := userFlags()
	for name, value := range map[string]int{"rapid.checks": checks, "rapid.steps": steps} {
		if asked[name] {
			continue
		}
		old := flag.Lookup(name).Value.String()
		if err := flag.Set(name, strconv.Itoa(value)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = flag.Set(name, old) })
	}
}

// ---- the systems ----

func TestConformMemorystore(t *testing.T) {
	keys := append(append(slices.Clone(topicRules), groupRules...), "createTopic/ErrTopicExists", "find.edge/OK")
	checkReaching(t, options{groups: true}, func(*rapid.T) runtime.OperationRunner { return memorystore.New() }, keys...)
}

// TestConformModelTwice is the determinism check of the model: two models that
// get the same operations give the same answers.
func TestConformModelTwice(t *testing.T) {
	check(t, options{groups: true}, func(*rapid.T) runtime.OperationRunner { return modelRunner{spec.New()} })
}

type modelRunner struct{ m *spec.ControlPlane }

func (r modelRunner) Exec(_ context.Context, op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	return r.m.Apply(op), nil
}

func TestConformMetadataMemory(t *testing.T) {
	check(t, options{groups: true}, func(*rapid.T) runtime.OperationRunner {
		return &metadatastore.Store{A: metadata.New(kv.NewMemory())}
	})
}

func TestConformMetadataPebble(t *testing.T) {
	setRapid(t, 20, 20)
	dir, n := t.TempDir(), 0
	check(t, options{groups: true}, func(rt *rapid.T) runtime.OperationRunner {
		n++
		store, err := pebblestore.Open(filepath.Join(dir, strconv.Itoa(n)), pebblestore.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
		if err != nil {
			rt.Fatal(err)
		}
		rt.Cleanup(func() { _ = store.Close() })
		return &metadatastore.Store{A: metadata.New(store)}
	})
}

// TestConformGRPC runs the real gRPC stack: client, server, replica, in-memory
// log. The wire API has no install or assign, so the alphabet is the topics
// operations.
func TestConformGRPC(t *testing.T) {
	setRapid(t, 20, 15)
	check(t, options{}, func(rt *rapid.T) runtime.OperationRunner {
		node := StartMem(t)
		rt.Cleanup(node.stop)
		return grpcRunner{node.Client}
	})
}

type grpcRunner struct{ c *Client }

func (g grpcRunner) Exec(ctx context.Context, op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	r, err := g.c.Call(ctx, op)
	if be, ok := errors.AsType[*Error](err); ok {
		return runtime.ControlPlaneDone{Err: be.Code}, nil
	}
	return runtime.ControlPlaneDone{Resp: r}, err
}

// maxExtraRounds bounds how many more rounds of checks checkReaching runs when
// an answer that the alphabet aims at was not reached yet.
const maxExtraRounds = 20

// checkReaching runs check, and runs more rounds of the same property checks
// until every key was reached (at most maxExtraRounds more). The draws are
// random and a rare answer such as a replay can be missed in one round of 30
// checks, which made this evidence fail about one run in five. Every round
// still judges the full property, so no check gets weaker. If a key is still
// missing after all rounds, requireReached fails the test: the alphabet does
// not reach it.
func checkReaching(t *testing.T, o options, newSystem func(*rapid.T) runtime.OperationRunner, keys ...string) {
	t.Helper()
	seen := check(t, o, newSystem)
	for range maxExtraRounds {
		if !missing(seen, keys) {
			break
		}
		for k, v := range check(t, o, newSystem) {
			seen[k] += v
		}
	}
	requireReached(t, seen, keys...)
}

// missing reports that some key was not seen.
func missing(seen map[string]int, keys []string) bool {
	return slices.ContainsFunc(keys, func(k string) bool { return seen[k] == 0 })
}

// requireReached is the evidence for the choices in alphabet_test.go: every
// answer in keys (action and code) was seen at least once. It needs enough
// checks, so a short run (a small -rapid.checks) skips it.
func requireReached(t *testing.T, seen map[string]int, keys ...string) {
	t.Helper()
	if n, _ := strconv.Atoi(flag.Lookup("rapid.checks").Value.String()); n < 20 {
		t.Skip("too few checks to count the answers")
	}
	for _, key := range keys {
		if seen[key] == 0 {
			t.Errorf("no %q in any check: the alphabet does not reach it", key)
		}
	}
}

// topicRules and groupRules are the answers that the choices in the alphabet
// aim at.
var topicRules = []string{
	"find/OK", "find/ErrInvalid", "describeTopics/OK",
	"commit/OK", "commit.replay/OK", "commit.conflict/ErrConflict", "commit/ErrInvalid",
	"createTopic/OK", "createTopic/ErrInvalid",
}

var groupRules = []string{
	"groupInstall/OK", "groupInstall/ErrIllegalGeneration", "groupInstall/ErrInvalid",
	"groupAssign/OK", "groupAssign/ErrUnknownMember", "groupAssign/ErrIllegalGeneration",
	"offsetCommit/OK", "offsetCommit/ErrIllegalGeneration", "offsetCommit/ErrUnknownMember",
	"groupRemove/OK", "groupAssign.tooLarge/ErrTooLarge", "describeGroup/OK",
}
