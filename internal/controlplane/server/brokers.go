package server

// Broker membership: each broker sends BrokerHeartbeat to renew its lease, and
// the active node answers with the live broker set (decision Q4, Q7b).
//
// Where the pieces live:
//   - wire messages: api/v1 (BrokerInfo, BrokerHeartbeat*)
//   - conversions: wireboundary, section "Broker heartbeat"
//   - membership: the lease rules (who is alive)
//   - this file: the handler
//   - broker side: system/production/lease.go sends the heartbeat and turns the
//     answer into a runtime.LiveBrokers event
//
// The set is not logged and not stored. After a restart or a failover it starts
// empty and fills again within one heartbeat interval.

import (
	"context"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/membership"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// BrokerHeartbeat implements ControlPlane. It renews the caller's lease and
// returns the live broker set (Q4). Only the active node keeps the set, so a
// standby answers Unavailable and the broker client tries the next address.
func (n *Node) BrokerHeartbeat(ctx context.Context, request *cpv1.BrokerHeartbeatRequest) (*cpv1.BrokerHeartbeatResponse, error) {
	b := wireboundary.BrokerHeartbeatToCore(request)
	if !membership.Valid(b) {
		return nil, toRPCError(runtime.ErrInvalid, runtime.NotApplied)
	}
	type result struct {
		live []contract.BrokerAddr
		ok   bool
	}
	ch := make(chan result, 1)
	err := n.onLoop(ctx, func(m *replica.Replica) []any {
		if !m.IsActive() || !m.Ready() {
			ch <- result{}
			return nil
		}
		now := n.config.Clock.Mono()
		n.leases.Renew(b, now)
		ch <- result{live: n.leases.Live(now), ok: true}
		return nil
	})
	if err != nil {
		return nil, toRPCError(runtime.ErrIO, runtime.NotApplied)
	}
	r := <-ch
	if !r.ok {
		return nil, toRPCError(runtime.ErrUnavailable, runtime.NotApplied)
	}
	return wireboundary.BrokerHeartbeatRespFromCore(r.live), nil
}
