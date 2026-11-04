package simulator

import (
	"fmt"
	"testing"

	"github.com/manifesterio/dst/golden"
)

// goldenHashes pins named scenario/seed traces. An accidental determinism
// change fails this test in review; bump a pin only after intentional review.
var goldenHashes = map[string]uint64{
	"basic/17": 0xf79ae0cb6ac19047,
	"chaos/42": 0x841e92a10f1559f3,
}

// TestGoldenTraceHashes pins a few seeds so a silent determinism drift fails CI.
// TestSameSeedSameHash only compares a seed with itself.
func TestGoldenTraceHashes(t *testing.T) {
	cases := []struct {
		scenario string
		seed     uint64
	}{
		{"basic", 17},
		{"chaos", 42},
	}
	for _, c := range cases {
		name := fmt.Sprintf("%s/%d", c.scenario, c.seed)
		r := mustRun(t, Config{Scenario: c.scenario, Seed: c.seed})
		if !r.OK() {
			t.Fatal(Report(r))
		}
		golden.Assert(t, name, r.Hash, goldenHashes)
	}
}
