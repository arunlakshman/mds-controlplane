package controlplaneclient

import (
	"context"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/wireboundary"
)

// Broker membership, client side. The server side is server/brokers.go.

// BrokerHeartbeat renews this broker's lease and returns the live broker set.
// A standby control plane answers ErrUnavailable, so the call moves to the next
// address.
func (c *Client) BrokerHeartbeat(ctx context.Context, self contract.BrokerAddr) ([]contract.BrokerAddr, error) {
	return callRPC(ctx, c, "BrokerHeartbeat", cpv1.ControlPlaneClient.BrokerHeartbeat,
		wireboundary.BrokerHeartbeatFromCore(self), wireboundary.BrokerHeartbeatRespToCore)
}
