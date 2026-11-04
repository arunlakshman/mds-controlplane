package simulator

import (
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func TestRandSameSeedSameStream(t *testing.T) {
	a, b := NewRand(42), NewRand(42)
	for range 100 {
		if a.Uint64() != b.Uint64() {
			t.Fatal("streams differ")
		}
	}
	if NewRand(1).Uint64() == NewRand(2).Uint64() {
		t.Fatal("different seeds gave the same first value")
	}
}

func TestRandRanges(t *testing.T) {
	r := NewRand(5)
	for range 1000 {
		if v := r.Intn(7); v < 0 || v >= 7 {
			t.Fatalf("Intn out of range: %d", v)
		}
		if v := r.Between(3, 5); v < 3 || v > 5 {
			t.Fatalf("Between out of range: %d", v)
		}
	}
	before := r.Calls()
	if r.Between(4, 4) != 4 || r.Intn(0) != 0 || r.Chance(0) {
		t.Fatal("degenerate ranges must return the low end")
	}
	if r.Calls() != before {
		t.Fatal("degenerate calls must not draw")
	}
}

// The clock moves only when an event is taken, and events run in time order.
func TestSchedulerOrderAndClock(t *testing.T) {
	s := newScheduler(NewRand(1))
	var got []int
	s.after(30, "c", func() { got = append(got, 3) })
	s.after(10, "a", func() { got = append(got, 1) })
	s.after(20, "b", func() {
		got = append(got, 2)
		s.after(5, "d", func() { got = append(got, 25) })
	})
	if s.Mono() != 0 {
		t.Fatal("the clock moved before any event")
	}
	for s.next() {
	}
	want := []int{1, 2, 25, 3}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if s.Mono() != 30 || s.Wall() != runtime.RealTime(wallStart+30) {
		t.Fatalf("clock is %d / %d", s.Mono(), s.Wall())
	}
}

// Events at the same time are reordered by the seed, and the same seed gives
// the same order.
func TestSchedulerTieBreakIsSeeded(t *testing.T) {
	order := func(seed uint64) string {
		s := newScheduler(NewRand(seed))
		var out []byte
		for i := range 8 {
			s.after(10, "x", func() { out = append(out, byte('a'+i)) })
		}
		for s.next() {
		}
		return string(out)
	}
	first := order(1)
	if again := order(1); first != again {
		t.Fatal("same seed, different order")
	}
	seen := map[string]bool{}
	for seed := uint64(1); seed <= 10; seed++ {
		seen[order(seed)] = true
	}
	if len(seen) < 2 {
		t.Fatal("the seed never changed the order of events at the same time")
	}
}
