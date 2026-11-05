package main

import (
	"fmt"
	"io"
	"slices"
	"strings"

	dstcampaign "github.com/manifesterio/dst/campaign"
)

// Report is the end-of-campaign picture: what ran, what failed, which
// assertions held, and whether more seeds still find new assertions.
type Report struct {
	Seeds        int           `json:"seeds"`  // seed numbers run (each runs once per scenario)
	Runs         int           `json:"runs"`   // seeds times scenarios
	Passed       int           `json:"passed"` // runs that kept every property
	Failed       int           `json:"failed"` // runs that broke a property
	FailedRuns   []FailedRun   `json:"failed_runs"`
	Assertions   []AssertRow   `json:"assertions"`    // always, sometimes and reachable, by name
	NeverReached []string      `json:"never_reached"` // expected or sometimes paths that no seed passed
	Curve        []CurvePoint  `json:"curve"`         // seed numbers where a new assertion appeared
	LastGrowth   uint64        `json:"last_growth"`   // seed number of the last new assertion (0: none)
	LastSeed     uint64        `json:"last_seed"`     // last seed number run
	UntilStable  *StableResult `json:"until_stable,omitempty"`
}

// FailedRun is a run that broke a property, with the command that replays it.
type FailedRun struct {
	Scenario string `json:"scenario"`
	Seed     uint64 `json:"seed"`
	Replay   string `json:"replay"`
}

// AssertRow is the count of one assertion over the whole campaign.
type AssertRow struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Checks uint64 `json:"checks"`
	Passes uint64 `json:"passes"`
}

// CurvePoint says that at seed number Seed the campaign had seen Total
// different assertions.
type CurvePoint struct {
	Seed  uint64 `json:"seed"`
	Total int    `json:"total"`
}

// StableResult is set when -until-stable was used.
type StableResult struct {
	StableAfter int  `json:"stable_after"`
	MaxSeeds    int  `json:"max_seeds"`
	HitLimit    bool `json:"hit_limit"` // the limit came first: the campaign was not stable yet
}

// buildReport folds the campaign. seen[i] lists the assertions that were
// reached by seed number first+i in any scenario.
func buildReport(first uint64, seen [][]string, total dstcampaign.Summary, failed []FailedRun, runs int) Report {
	r := Report{Seeds: len(seen), Runs: runs, Failed: len(failed), Passed: runs - len(failed), FailedRuns: failed}
	if r.FailedRuns == nil {
		r.FailedRuns = []FailedRun{}
	}
	r.Curve, r.LastGrowth = coverageCurve(first, seen)
	if len(seen) > 0 {
		r.LastSeed = first + uint64(len(seen)) - 1
	}
	names := make([]string, 0, len(total.Coverage))
	for n := range total.Coverage {
		names = append(names, n)
	}
	slices.Sort(names)
	r.Assertions = []AssertRow{}
	r.NeverReached = []string{}
	for _, n := range names {
		c := total.Coverage[n]
		r.Assertions = append(r.Assertions, AssertRow{Name: n, Kind: c.Kind, Checks: c.Checks, Passes: c.Passes})
		if c.Kind == "sometimes" && c.Passes == 0 {
			r.NeverReached = append(r.NeverReached, n)
		}
	}
	// A path that never ran leaves no entry at all, so only the hand-kept
	// list can name it.
	for _, n := range dstcampaign.Missing(total.Coverage, expectedReachable) {
		if !slices.Contains(r.NeverReached, n) {
			r.NeverReached = append(r.NeverReached, n)
		}
	}
	slices.Sort(r.NeverReached)
	return r
}

// coverageCurve returns one point for each seed number that saw an assertion
// for the first time, and the last such seed number.
func coverageCurve(first uint64, seen [][]string) ([]CurvePoint, uint64) {
	have := map[string]bool{}
	curve := []CurvePoint{}
	var last uint64
	for i, names := range seen {
		grew := false
		for _, n := range names {
			if !have[n] {
				have[n] = true
				grew = true
			}
		}
		if grew {
			last = first + uint64(i)
			curve = append(curve, CurvePoint{Seed: last, Total: len(have)})
		}
	}
	return curve, last
}

// quietSeeds is how many seed numbers at the end of seen added no new assertion.
func quietSeeds(first uint64, seen [][]string) int {
	_, last := coverageCurve(first, seen)
	if last == 0 {
		return len(seen)
	}
	return int(first + uint64(len(seen)) - 1 - last)
}

// writeText prints the report. Failed seeds come first.
func (r Report) writeText(w io.Writer) {
	p := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format+"\n", a...) }
	p("")
	p("report: %d seeds, %d runs, %d passed, %d failed", r.Seeds, r.Runs, r.Passed, r.Failed)
	for _, f := range r.FailedRuns {
		p("  FAILED %s seed %d: %s", f.Scenario, f.Seed, f.Replay)
	}
	p("assertions (%d):", len(r.Assertions))
	for _, a := range r.Assertions {
		p("  %-10s %-60s checks %9d  passes %9d", a.Kind, a.Name, a.Checks, a.Passes)
	}
	if len(r.NeverReached) == 0 {
		p("never reached: none")
	} else {
		p("never reached:\n  %s", strings.Join(r.NeverReached, "\n  "))
	}
	p("coverage curve (seed: assertions seen so far):")
	for _, c := range r.Curve {
		p("  seed %6d: %d", c.Seed, c.Total)
	}
	if r.LastGrowth == 0 {
		p("no assertion was seen")
	} else {
		p("stopped growing at seed %d (last seed run: %d)", r.LastGrowth, r.LastSeed)
	}
	if u := r.UntilStable; u != nil && u.HitLimit {
		p("WARNING: the limit of %d seeds came first. No %d seeds in a row without a new assertion was reached, so the campaign is not known to be stable.", u.MaxSeeds, u.StableAfter)
	}
}
