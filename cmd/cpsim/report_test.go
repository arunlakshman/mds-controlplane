package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	dstcampaign "github.com/manifesterio/dst/campaign"
)

func TestCoverageCurve(t *testing.T) {
	seen := [][]string{{"a"}, {"a"}, {"a", "b"}, {"b"}, {"b"}}
	curve, last := coverageCurve(10, seen)
	if last != 12 || len(curve) != 2 || curve[0] != (CurvePoint{10, 1}) || curve[1] != (CurvePoint{12, 2}) {
		t.Fatalf("curve %v last %d", curve, last)
	}
	if q := quietSeeds(10, seen); q != 2 {
		t.Fatalf("quiet seeds %d, want 2", q)
	}
}

// TestReportNamesLostPath: when a path that the campaign used to reach is no
// longer reached, the report lists it and the coverage line says so.
func TestReportNamesLostPath(t *testing.T) {
	cov := map[string]dstcampaign.Coverage{}
	for _, n := range expectedReachable {
		cov[n] = dstcampaign.Coverage{Kind: "reachable", Checks: 1, Passes: 1}
	}
	lost := expectedReachable[3]
	delete(cov, lost)
	total := dstcampaign.Summary{Coverage: cov}
	rep := buildReport(1, [][]string{{"x"}}, total, nil, 1)
	if len(rep.NeverReached) != 1 || rep.NeverReached[0] != lost {
		t.Fatalf("never reached %v, want [%s]", rep.NeverReached, lost)
	}
	if text := coverageText(total); !strings.Contains(text, lost) {
		t.Fatalf("coverage text does not name %q: %s", lost, text)
	}
}

func TestReportFailedSeedsFirst(t *testing.T) {
	rep := buildReport(1, [][]string{{"x"}, {"x"}}, dstcampaign.Summary{}, []FailedRun{{"chaos", 2, "go run ./cmd/cpsim -scenario chaos -seed 2"}}, 2)
	var b bytes.Buffer
	rep.writeText(&b)
	lines := strings.Split(b.String(), "\n")
	if !strings.Contains(lines[1], "1 failed") || !strings.Contains(lines[2], "FAILED chaos seed 2") {
		t.Fatalf("failed seeds are not first:\n%s", b.String())
	}
}

func TestRunJSONAndUntilStable(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"-seeds", "3", "-scenario", "basic", "-json"}, &out, &errOut); code != 0 {
		t.Fatalf("code %d: %s", code, errOut.String())
	}
	var rep Report
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("not json: %v\n%s", err, out.String())
	}
	if rep.Seeds != 3 || rep.Runs != 3 || rep.Passed != 3 || len(rep.Assertions) == 0 {
		t.Fatalf("report %+v", rep)
	}

	out.Reset()
	if code := run([]string{"-until-stable", "-scenario", "basic", "-stable-after", "50", "-max-seeds", "50"}, &out, &errOut); code != 0 {
		t.Fatalf("code %d: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "WARNING: the limit of 50 seeds came first") {
		t.Fatalf("no warning:\n%s", out.String())
	}
}
