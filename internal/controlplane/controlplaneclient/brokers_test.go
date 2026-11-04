package controlplaneclient

import (
	"errors"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func TestClientBrokerHeartbeat(t *testing.T) {
	c := realClient(t)
	ctx := t.Context()
	b2 := contract.BrokerAddr{NodeID: 2, Host: "h2", Port: 9092}
	b1 := contract.BrokerAddr{NodeID: 1, Host: "h1", Port: 9092}
	// The first beat sees only itself. The second sees both, sorted by node id.
	live, err := c.BrokerHeartbeat(ctx, b2)
	if err != nil || len(live) != 1 || live[0] != b2 {
		t.Fatalf("first beat: %v %v", live, err)
	}
	live, err = c.BrokerHeartbeat(ctx, b1)
	if err != nil || len(live) != 2 || live[0] != b1 || live[1] != b2 {
		t.Fatalf("second beat: %v %v", live, err)
	}
	_, err = c.BrokerHeartbeat(ctx, contract.BrokerAddr{NodeID: 0, Host: "h", Port: 1})
	if be, ok := errors.AsType[*Error](err); !ok || be.Code != runtime.ErrInvalid {
		t.Fatalf("node id 0: %v", err)
	}
}
