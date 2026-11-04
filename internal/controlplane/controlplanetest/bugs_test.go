package controlplanetest

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"testing"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/controlplanetest/memorystore"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Planted bugs. Each test wraps a correct system with one known bug and asserts
// that the conformance test FAILS. A pass of the real tests means something
// only if these tests show that the same property test can find a bug.

// recorder is a testing.TB that does not fail the real test. rapid reports its
// failure to the recorder, and the test then checks that it was told.
type recorder struct {
	*testing.T
	failed bool
}

func (r *recorder) Errorf(string, ...any) { r.failed = true }
func (r *recorder) Error(...any)          { r.failed = true }
func (r *recorder) Fatalf(string, ...any) { r.failed = true }
func (r *recorder) Fatal(...any)          { r.failed = true }
func (r *recorder) Fail()                 { r.failed = true }
func (r *recorder) FailNow()              { r.failed = true }
func (r *recorder) Failed() bool          { return r.failed }
func (r *recorder) Logf(string, ...any)   {}
func (r *recorder) Log(...any)            {}

// requireFound runs the conformance test and fails the real test if the
// conformance test passes.
func requireFound(t *testing.T, o options, newSystem func(*rapid.T) runtime.OperationRunner) {
	t.Helper()
	// An expected failure leaves no fail file, and it needs no long shrinking.
	for name, value := range map[string]string{"rapid.nofailfile": "true", "rapid.shrinktime": "1s"} {
		old := flag.Lookup(name).Value.String()
		if err := flag.Set(name, value); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = flag.Set(name, old) }()
	}
	r := &recorder{T: t}
	check(r, o, newSystem)
	if !r.failed {
		t.Fatal("the planted bug was not found")
	}
}

// buggy is a system with one change on the way in or on the way out.
type buggy struct {
	inner runtime.OperationRunner
	in    func(runtime.ControlPlaneOperation) runtime.ControlPlaneOperation
	out   func(runtime.ControlPlaneOperation, runtime.ControlPlaneDone) runtime.ControlPlaneDone
}

func (b *buggy) Exec(ctx context.Context, op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	if b.in != nil {
		op = b.in(op)
	}
	d, err := b.inner.Exec(ctx, op)
	if b.out != nil {
		d = b.out(op, d)
	}
	return d, err
}

func plant(b *buggy) func(*rapid.T) runtime.OperationRunner {
	return func(*rapid.T) runtime.OperationRunner {
		b.inner = memorystore.New()
		return b
	}
}

// The store forgets the segment key: a replay is a new segment with new offsets.
func TestPlantedBugReplayGetsNewOffsets(t *testing.T) {
	n := 0
	requireFound(t, options{}, plant(&buggy{in: func(op runtime.ControlPlaneOperation) runtime.ControlPlaneOperation {
		if c, ok := op.(contract.CommitSegment); ok {
			n++
			c.SegmentKey = fmt.Sprintf("%s#%d", c.SegmentKey, n)
			return c
		}
		return op
	}}))
}

// The stop rule of FindSegments is off by one: it uses MaxBytes+1.
func TestPlantedBugMaxBytesOffByOne(t *testing.T) {
	requireFound(t, options{}, plant(&buggy{in: func(op runtime.ControlPlaneOperation) runtime.ControlPlaneOperation {
		if f, ok := op.(contract.FindSegments); ok && f.MaxBytes > 0 {
			f.MaxBytes++
			return f
		}
		return op
	}}))
}

// DescribeTopics is not sorted by name.
func TestPlantedBugTopicsNotSorted(t *testing.T) {
	requireFound(t, options{}, plant(&buggy{out: func(_ runtime.ControlPlaneOperation, d runtime.ControlPlaneDone) runtime.ControlPlaneDone {
		if r, ok := d.Resp.(contract.DescribeTopicsResp); ok {
			slices.Reverse(r.Topics)
			d.Resp = r
		}
		return d
	}}))
}

// forgetful is a replica on a disk with a write-back cache and no flush: the
// last log entries were answered but they were still in the cache, so a crash
// loses them. This breaks persistence.
type forgetful struct{ *nodeSystem }

func (f forgetful) DirtyReboot(keep int) {
	s := f.nodeSystem
	s.proc.dead.Store(true)
	_ = s.node.Close()
	ctx := s.t.Context()
	entries, _ := s.disk.log.Read(ctx, 0, 1<<20)
	epoch, owner, _, _ := s.disk.log.ClaimEpoch(ctx, 1<<62, "")
	lost := executor.NewMemoryLog()
	for _, e := range entries[:max(0, len(entries)-2)] {
		_, _, _ = lost.Append(ctx, e.Slot, e.Data)
	}
	for i := range epoch {
		_, _, _, _ = lost.ClaimEpoch(ctx, i, owner)
	}
	s.disk.log = lost
	s.DirtyReboot(keep)
}

func TestPlantedBugCrashLosesAnsweredWrites(t *testing.T) {
	setRapid(t, 100, 20)
	requireFound(t, options{restart: true, dirty: true}, func(rt *rapid.T) runtime.OperationRunner {
		return forgetful{newNodeSystem(rt, false)}
	})
}

// staleReads answers a read with "no data" after a failure was injected. The
// "has failed" flag allows an error there, but never an answer that is wrong.
type staleReads struct{ *nodeSystem }

func (s staleReads) Exec(ctx context.Context, op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	if _, ok := op.(contract.FindSegments); ok && s.injected {
		return runtime.ControlPlaneDone{Resp: contract.FindSegmentsResp{}}, nil
	}
	return s.nodeSystem.Exec(ctx, op)
}

func TestPlantedBugNoDataAfterFailure(t *testing.T) {
	setRapid(t, 100, 20)
	requireFound(t, options{retry: true, faults: true, restart: true}, func(rt *rapid.T) runtime.OperationRunner {
		return staleReads{newNodeSystem(rt, false)}
	})
}
