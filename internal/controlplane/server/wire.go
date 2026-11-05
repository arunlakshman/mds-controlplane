package server

import (
	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Conversions shared by every context: error codes and outcomes, and the
// types that more than one context uses (topic partitions, segment references,
// batches and ranges).

// ---- Error and outcome codes ----

// fromCoreOutcome maps runtime.Outcome to the wire enum.
func fromCoreOutcome(o runtime.Outcome) cpv1.Outcome {
	switch o {
	case runtime.MaybeApplied:
		return cpv1.Outcome_OUTCOME_MAYBE_APPLIED
	case runtime.Applied:
		return cpv1.Outcome_OUTCOME_APPLIED
	default:
		return cpv1.Outcome_OUTCOME_NOT_APPLIED
	}
}

// ---- Shared pieces ----

// Rule for every "ToCore" function in this package: read the wire message with
// its Get methods. They accept a nil message and give the zero value, so a
// missing part becomes a zero value and nothing panics.

// mapSlice converts every element. An empty input gives nil, the same as a
// loop that appends to a nil slice.
func mapSlice[From, To any](in []From, convert func(From) To) []To {
	if len(in) == 0 {
		return nil
	}
	out := make([]To, len(in))
	for i, item := range in {
		out[i] = convert(item)
	}
	return out
}
