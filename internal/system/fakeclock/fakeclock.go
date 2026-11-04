// Package fakeclock is the test and simulation implementation of runtime.Clock.
// It wraps dst/clock over a private sched heap. Time moves only when the
// caller calls Advance. A mutex serializes Advance against AfterFunc from a
// run-loop goroutine; this package is not in the determinism gate.
package fakeclock

import (
	"sync"
	"time"

	dstclock "github.com/arunlakshman/dst/clock"
	"github.com/arunlakshman/dst/rng"
	"github.com/arunlakshman/dst/sched"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Clock is a runtime.Clock that moves only on Advance.
type Clock struct {
	mu      sync.Mutex
	s       *sched.Scheduler
	inner   *dstclock.Clock
	pending int
}

type timer struct {
	clock *Clock
	inner *dstclock.Timer
}

var _ runtime.Clock = (*Clock)(nil)

// New returns a Clock whose Wall time starts at wall. Mono starts at one
// second, so that zero can mean "not set" in the code under test.
func New(wall time.Time) *Clock {
	monoStart := sched.TimeNs(time.Second)
	// wallStart + Mono == wall.UnixNano() at construction.
	wallStart := sched.TimeNs(wall.UnixNano()) - monoStart
	s := sched.New(rng.New(1))
	s.Jump(monoStart)
	return &Clock{s: s, inner: dstclock.New(s, wallStart)}
}

// Mono returns the node-local time.
func (c *Clock) Mono() runtime.NodeTime {
	c.mu.Lock()
	defer c.mu.Unlock()
	return runtime.NodeTime(c.inner.Mono())
}

// Wall returns the UTC time.
func (c *Clock) Wall() runtime.RealTime {
	c.mu.Lock()
	defer c.mu.Unlock()
	return runtime.RealTime(c.inner.Wall())
}

// AfterFunc schedules f for d after now. It runs inside a later Advance.
func (c *Clock) AfterFunc(d runtime.Duration, f func()) runtime.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending++
	tm := &timer{clock: c}
	tm.inner = c.inner.AfterFunc(sched.TimeNs(d), func() {
		c.mu.Lock()
		c.pending--
		c.mu.Unlock()
		f()
	})
	return tm
}

func (t *timer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if !t.inner.Stop() {
		return false
	}
	t.clock.pending--
	return true
}

// Advance moves time forward by d and fires every timer that is due, in time
// order. A timer may start new timers; they fire in the same call if they are due.
// The heap is driven under mu via Take so callbacks can call AfterFunc without deadlock.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.s.Mono() + sched.TimeNs(d)
	for {
		at, ok := c.s.Peek()
		if !ok || at > target {
			c.s.Jump(target)
			c.mu.Unlock()
			return
		}
		run, ok := c.s.Take()
		if !ok {
			c.s.Jump(target)
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		run()
		c.mu.Lock()
	}
}

// Pending counts the timers that were not stopped and did not fire yet.
func (c *Clock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pending
}
