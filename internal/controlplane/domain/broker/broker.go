// Package broker is the broker context of the control plane:
// which brokers are alive. Each broker sends a heartbeat to renew its lease, and
// the active control-plane node answers with the live broker set (decision Q4,
// Q7b).
//
// The set is not logged and not stored, so it is not a module of the replicated
// state. After a restart or a failover it starts empty and fills again within one
// heartbeat interval. It has no lock: the caller uses it only on the run loop.
//
// Time comes from the caller, as runtime.NodeTime, so the rules are the same with
// the real clock and the fake one.
package broker

import (
	"cmp"
	"slices"
	"time"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// DefaultTTL is the lease length when the configuration leaves it zero. Brokers
// send a heartbeat every interval, so keep the lease at about 3 times it.
const DefaultTTL = 5 * time.Second

// Valid reports whether b is an address a broker may register: a positive node
// id, a host and a positive port.
func Valid(b contract.BrokerAddr) bool {
	return b.NodeID > 0 && b.Host != "" && b.Port > 0
}

// Leases holds the leases of the brokers that sent a heartbeat recently.
type Leases struct {
	ttl    time.Duration
	leases map[int32]lease // by node id
}

type lease struct {
	addr contract.BrokerAddr
	seen runtime.NodeTime // time of the last heartbeat
}

// New returns an empty set. A lease lives for ttl after its last heartbeat.
func New(ttl time.Duration) *Leases {
	return &Leases{ttl: ttl, leases: map[int32]lease{}}
}

// Renew records a heartbeat from b at now. A new address for a known node id
// replaces the old one.
func (s *Leases) Renew(b contract.BrokerAddr, now runtime.NodeTime) {
	s.leases[b.NodeID] = lease{addr: b, seen: now}
}

// Live drops the leases that expired by now and returns the rest, sorted by
// node id.
func (s *Leases) Live(now runtime.NodeTime) []contract.BrokerAddr {
	var out []contract.BrokerAddr
	for id, l := range s.leases {
		if time.Duration(now-l.seen) >= s.ttl {
			delete(s.leases, id)
			continue
		}
		out = append(out, l.addr)
	}
	slices.SortFunc(out, func(a, b contract.BrokerAddr) int {
		return cmp.Compare(a.NodeID, b.NodeID)
	})
	return out
}
