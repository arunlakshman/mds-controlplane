package simulator

import (
	"github.com/manifesterio/dst/rng"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Rand is the one seeded random stream of a run. It embeds dst/rng and also
// implements runtime.Rand, so the replicas draw from the same stream.
type Rand struct {
	*rng.Rand
}

// NewRand returns a stream for seed.
func NewRand(seed uint64) *Rand { return &Rand{Rand: rng.New(seed)} }

// NewID returns a fresh 128-bit id (runtime.Rand).
func (r *Rand) NewID() runtime.ID {
	var id runtime.ID
	r.Bytes(id[:])
	return id
}
