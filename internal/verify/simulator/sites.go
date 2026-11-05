package simulator

// The buggify sites of the simulator. Each is a place where the fake log or
// the fake object store does something that the real one may do too. A site is
// switched on for some runs and off for others, and when on it fires now and
// then (dst fault.Injector.Maybe). The names are stable plain strings: a report
// and a replay refer to them.
const (
	// SiteLogSlowAnswer: the log applies the call, and the reply comes back late.
	SiteLogSlowAnswer = "sim.log.slow-answer"
	// SiteLogShortPage: a LogRead returns fewer entries than it could (at least
	// one). LogRead promises "up to Limit", and the reader asks again from the
	// slot after the last entry it got.
	SiteLogShortPage = "sim.log.short-read-page"
	// SiteObjectsSlowAnswer: the object store applies the call, and the reply
	// comes back late.
	SiteObjectsSlowAnswer = "sim.objects.slow-answer"
)

// SiteNames returns the names of all buggify sites of the simulator, in order.
func SiteNames() []string {
	return []string{SiteLogSlowAnswer, SiteLogShortPage, SiteObjectsSlowAnswer}
}
