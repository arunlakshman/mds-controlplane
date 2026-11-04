package simulator

import (
	"fmt"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// traceRing is how many of the last steps a run keeps for a failure report.
const traceRing = 48

// traceLine is one step, kept as plain data and formatted only when needed.
type traceLine struct {
	step int
	at   int64
	node int
	body runtime.EventData
	acts int
}

func (t traceLine) String() string {
	return fmt.Sprintf("step %d t=%dms cp-%d <- %s (%d actions)", t.step, t.at/int64(runtime.Millisecond), t.node, bodyText(t.body), t.acts)
}

// bodyText is a short text of an event. Long byte slices are cut.
func bodyText(b runtime.EventData) string {
	s := fmt.Sprintf("%T%+v", b, b)
	const limit = 160
	if len(s) > limit {
		s = s[:limit] + "..."
	}
	return s
}

// tracef writes a note about something that is not a step (a crash, a partition).
func (s *sim) tracef(format string, args ...any) {
	if s.cfg.Trace != nil {
		s.cfg.Trace(fmt.Sprintf("t=%dms %s", s.sched.now/int64(runtime.Millisecond), fmt.Sprintf(format, args...)))
	}
}

// tail returns the kept trace lines, oldest first.
func (s *sim) tail() []string {
	first := max(0, s.rpos-traceRing)
	out := make([]string, 0, s.rpos-first)
	for i := first; i < s.rpos; i++ {
		out = append(out, s.ring[i%traceRing].String())
	}
	return out
}
