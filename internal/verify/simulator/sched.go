package simulator

import "github.com/manifesterio/mds-controlplane/internal/runtime"

// wallStart is the virtual UTC time at the start of every run (2026-01-01).
const wallStart = int64(1767225600) * int64(runtime.Second)

// event is one thing that happens at a virtual time. Events with the same time
// run in the order of tie, which is drawn from the seeded stream when the event
// is queued, so the scheduler reorders events that are ready together and the
// seed decides how. seq breaks a tie of ties and keeps the order total.
type event struct {
	at    int64
	tie   uint64
	seq   uint64
	label string
	run   func()
}

// eventHeap is a binary min-heap of events (container/heap is not on the
// allow-list of the determinism gate, and 20 lines are enough).
type eventHeap []*event

func (h eventHeap) less(i, j int) bool {
	a, b := h[i], h[j]
	if a.at != b.at {
		return a.at < b.at
	}
	if a.tie != b.tie {
		return a.tie < b.tie
	}
	return a.seq < b.seq
}

func (h *eventHeap) push(e *event) {
	*h = append(*h, e)
	q := *h
	for i := len(q) - 1; i > 0; {
		parent := (i - 1) / 2
		if !q.less(i, parent) {
			break
		}
		q[i], q[parent] = q[parent], q[i]
		i = parent
	}
}

func (h *eventHeap) pop() *event {
	q := *h
	top := q[0]
	last := len(q) - 1
	q[0], q[last] = q[last], nil
	q = q[:last]
	*h = q
	for i := 0; ; {
		small, l, r := i, 2*i+1, 2*i+2
		if l < len(q) && q.less(l, small) {
			small = l
		}
		if r < len(q) && q.less(r, small) {
			small = r
		}
		if small == i {
			break
		}
		q[i], q[small] = q[small], q[i]
		i = small
	}
	return top
}

// scheduler owns the virtual clock and the event queue. Mono and Wall move only
// when next takes an event.
type scheduler struct {
	rand  *Rand
	now   int64 // virtual monotonic time in nanoseconds since the start of the run
	seq   uint64
	steps int // events taken so far
	queue eventHeap
}

func newScheduler(rand *Rand) *scheduler { return &scheduler{rand: rand} }

// Mono is the virtual monotonic time.
func (s *scheduler) Mono() runtime.NodeTime { return runtime.NodeTime(s.now) }

// Wall is the virtual UTC time.
func (s *scheduler) Wall() runtime.RealTime { return runtime.RealTime(wallStart + s.now) }

// after queues run to happen d from now. A negative d counts as zero.
func (s *scheduler) after(d runtime.Duration, label string, run func()) {
	d = max(d, 0)
	s.seq++
	s.queue.push(&event{at: s.now + int64(d), tie: s.rand.Uint64(), seq: s.seq, label: label, run: run})
}

// next takes the first event, moves the clock to its time and runs it. It
// returns false when the queue is empty.
func (s *scheduler) next() bool {
	if len(s.queue) == 0 {
		return false
	}
	e := s.queue.pop()
	s.now = max(s.now, e.at)
	s.steps++
	e.run()
	return true
}
