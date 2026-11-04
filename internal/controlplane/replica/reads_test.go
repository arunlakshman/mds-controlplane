package replica_test

import (
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// A read that arrives while a tail check is in flight waits for the NEXT check.
func TestReadWaitsForFreshTail(t *testing.T) {
	m := newMachine(t, replica.Config{
		Writer: "s1", Rank: 1,
		CatchUpEvery:  runtime.Millisecond,
		TakeoverAfter: 10 * runtime.Second,
	})
	acts := bootEmpty(t, m, 0)
	timer, ok := findTimer(acts, 1)
	if !ok {
		t.Fatalf("catch-up timer missing in %v", acts)
	}
	tailA := m.Step(runtime.Event{At: 1, Body: runtime.TimerFired{ID: timer.ID, Gen: timer.Gen}})[0].(runtime.LogGetEnd)
	got := m.Step(runtime.Event{At: 2, Body: runtime.ControlPlaneDeliver{
		From: runtime.NodeInstance{Node: "b"}, Req: 7, Op: contract.DescribeTopics{},
	}})
	if len(got) != 0 {
		t.Fatalf("read answered at once: %v", got)
	}
	acts = m.Step(runtime.Event{At: 3, Body: runtime.LogGetEndDone{Req: tailA.Req, Err: runtime.OK, Outcome: runtime.Applied}})
	for _, a := range acts {
		if _, isReply := a.(runtime.ControlPlaneReply); isReply {
			t.Fatalf("read answered from a tail check that started before it: %v", acts)
		}
	}
	timer, ok = findTimer(acts, 1)
	if !ok {
		t.Fatalf("catch-up timer missing in %v", acts)
	}
	tailB := m.Step(runtime.Event{At: 4, Body: runtime.TimerFired{ID: timer.ID, Gen: timer.Gen}})[0].(runtime.LogGetEnd)
	acts = m.Step(runtime.Event{At: 5, Body: runtime.LogGetEndDone{Req: tailB.Req, Err: runtime.OK, Outcome: runtime.Applied}})
	for _, a := range acts {
		if r, isReply := a.(runtime.ControlPlaneReply); isReply && r.Done.Req == 7 && r.Done.Err == runtime.OK {
			return
		}
	}
	t.Fatalf("no reply after a fresh tail: %v", acts)
}

// A node that is not Ready refuses a read at once, so the bridge can fail over.
func TestReadRefusedWhenNotReady(t *testing.T) {
	m := newMachine(t, replica.Config{Writer: "s1", CatchUpEvery: runtime.Millisecond})
	m.Step(runtime.Event{Body: runtime.NodeStarted{}})
	acts := m.Step(runtime.Event{At: 1, Body: runtime.ControlPlaneDeliver{
		From: runtime.NodeInstance{Node: "b"}, Req: 8, Op: contract.DescribeTopics{},
	}})
	if len(acts) != 1 {
		t.Fatalf("want one reply, got %v", acts)
	}
	if r, ok := acts[0].(runtime.ControlPlaneReply); !ok || r.Done.Err != runtime.ErrUnavailable {
		t.Fatalf("want ErrUnavailable reply, got %v", acts[0])
	}
}
