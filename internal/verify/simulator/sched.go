package simulator

import (
	dstclock "github.com/arunlakshman/dst/clock"
	"github.com/arunlakshman/dst/sched"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// wallStart is the virtual UTC time at the start of every run (2026-01-01).
const wallStart = int64(1767225600) * int64(runtime.Second)

// scheduler shares one sched heap with dst/clock. Mono and Wall move only when
// next takes an event. after/next talk to the heap directly so CancelTimer
// stays a node-local token check, not clock.Timer.Stop.
type scheduler struct {
	s     *sched.Scheduler
	clock *dstclock.Clock
}

func newScheduler(r *Rand) *scheduler {
	s := sched.New(r.Rand)
	return &scheduler{s: s, clock: dstclock.New(s, sched.TimeNs(wallStart))}
}

// Mono is the virtual monotonic time.
func (s *scheduler) Mono() runtime.NodeTime { return runtime.NodeTime(s.clock.Mono()) }

// Wall is the virtual UTC time.
func (s *scheduler) Wall() runtime.RealTime { return runtime.RealTime(s.clock.Wall()) }

func (s *scheduler) now() int64 { return int64(s.clock.Mono()) }

func (s *scheduler) steps() int { return s.s.Steps() }

// after queues run to happen d from now. A negative d counts as zero.
func (s *scheduler) after(d runtime.Duration, label string, run func()) {
	s.s.After(sched.TimeNs(d), label, run)
}

// next takes the first event, moves the clock to its time and runs it. It
// returns false when the queue is empty.
func (s *scheduler) next() bool { return s.s.Next() }
