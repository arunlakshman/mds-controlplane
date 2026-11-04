package wireboundary

import (
	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
)

// Conversions for broker membership: BrokerHeartbeat.

func brokerInfoFromCore(b contract.BrokerAddr) *cpv1.BrokerInfo {
	return &cpv1.BrokerInfo{NodeId: b.NodeID, Host: b.Host, Port: b.Port}
}

func brokerInfoToCore(b *cpv1.BrokerInfo) contract.BrokerAddr {
	return contract.BrokerAddr{NodeID: b.GetNodeId(), Host: b.GetHost(), Port: b.GetPort()}
}

// ---- Data calls ----

// BrokerHeartbeatFromCore builds a BrokerHeartbeatRequest for a broker's own address.
func BrokerHeartbeatFromCore(b contract.BrokerAddr) *cpv1.BrokerHeartbeatRequest {
	return &cpv1.BrokerHeartbeatRequest{Broker: brokerInfoFromCore(b)}
}

// BrokerHeartbeatToCore rebuilds the calling broker's address from a request.
func BrokerHeartbeatToCore(request *cpv1.BrokerHeartbeatRequest) contract.BrokerAddr {
	return brokerInfoToCore(request.GetBroker())
}

// BrokerHeartbeatRespFromCore builds a BrokerHeartbeatResponse from the live set.
func BrokerHeartbeatRespFromCore(live []contract.BrokerAddr) *cpv1.BrokerHeartbeatResponse {
	return &cpv1.BrokerHeartbeatResponse{Brokers: mapSlice(live, brokerInfoFromCore)}
}

// BrokerHeartbeatRespToCore rebuilds the live set from a response.
func BrokerHeartbeatRespToCore(r *cpv1.BrokerHeartbeatResponse) []contract.BrokerAddr {
	return mapSlice(r.GetBrokers(), brokerInfoToCore)
}

// ---- Consumer group calls ----
