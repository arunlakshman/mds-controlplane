// Package realclock is the production implementation of runtime.Clock. It is
// nondeterministic by design and is not in the determinism gate.
package realclock

import (
	"time"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Clock is the process clock: Mono counts from the clock's creation using
// Go's monotonic reading, Wall is UTC.
type Clock struct{ start time.Time }

var _ runtime.Clock = Clock{}

// New returns a Clock anchored at the current time.
func New() Clock { return Clock{start: time.Now()} }

// Mono returns monotonic time elapsed since the clock was created.
func (c Clock) Mono() runtime.NodeTime { return runtime.NodeTime(time.Since(c.start)) }

// Wall returns the current wall-clock time in nanoseconds.
func (Clock) Wall() runtime.RealTime { return runtime.RealTime(time.Now().UnixNano()) }

// AfterFunc calls f once after d, on its own goroutine.
func (Clock) AfterFunc(d runtime.Duration, f func()) runtime.Timer {
	return time.AfterFunc(time.Duration(d), f)
}
