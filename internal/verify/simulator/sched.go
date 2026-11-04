package simulator

import (
	"github.com/arunlakshman/dst/sched"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// wallStart is the virtual UTC time at the start of every run (2026-01-01).
const wallStart = int64(1767225600) * int64(runtime.Second)

// scheduler wraps dst/sched. Mono and Wall move only when next takes an event.
type scheduler struct {
	inner *sched.Scheduler
}

func newScheduler(r *Rand) *scheduler {
	return &scheduler{inner: sched.New(r.Rand)}
}

// Mono is the virtual monotonic time.
func (s *scheduler) Mono() runtime.NodeTime { return runtime.NodeTime(s.inner.Mono()) }

// Wall is the virtual UTC time.
func (s *scheduler) Wall() runtime.RealTime {
	return runtime.RealTime(wallStart + int64(s.inner.Mono()))
}

func (s *scheduler) now() int64 { return int64(s.inner.Mono()) }

func (s *scheduler) steps() int { return s.inner.Steps() }

// after queues run to happen d from now. A negative d counts as zero.
func (s *scheduler) after(d runtime.Duration, label string, run func()) {
	s.inner.After(sched.TimeNs(d), label, run)
}

// next takes the first event, moves the clock to its time and runs it. It
// returns false when the queue is empty.
func (s *scheduler) next() bool { return s.inner.Next() }
