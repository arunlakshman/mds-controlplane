package controlplaneclient

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// deadClient points at an address where nothing listens, so every call fails
// in the transport and never reaches a node.
func deadClient(t *testing.T) *Client {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	c := New(addr, nil)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestCallGroupTransportErrors: each group op returns its transport error and
// no answer when no node can be reached.
func TestCallGroupTransportErrors(t *testing.T) {
	c := deadClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ops := []runtime.ControlPlaneOperation{
		contract.GroupJoin{Group: "g", ClientID: "c", ProtocolType: "consumer",
			Protocols: []contract.GroupProtocol{{Name: "range", Metadata: []byte("m")}}},
		contract.GroupSync{Group: "g", MemberID: "m", Generation: 1},
		contract.GroupHeartbeat{Group: "g", MemberID: "m", Generation: 1},
		contract.GroupLeave{Group: "g", Members: []string{"m"}},
		contract.OffsetCommit{Group: "g", Member: "m", Generation: 1},
		contract.OffsetFetch{Group: "g"},
		contract.DescribeGroups{Groups: []string{"g"}},
		contract.ListGroups{},
	}
	for _, operation := range ops {
		response, err := c.callGroup(ctx, operation)
		if err == nil {
			t.Fatalf("%T: no error, resp %+v", operation, response)
		}
		_ = response // OffsetCommit returns a zero response with its error, so the answer is not checked
		if code, _ := (Classifier{}).Classify(runtime.ControlPlaneCall{Op: operation}, err); code == runtime.OK {
			t.Fatalf("%T: classified as OK: %v", operation, err)
		}
	}
}

// TestCallGroupAnswers: the success path of the read ops gives the contract answer.
func TestCallGroupAnswers(t *testing.T) {
	c := realGroupClient(t)
	ctx := t.Context()
	if _, err := c.JoinGroup(ctx, joinOf("g-ans", "")); err != nil {
		t.Fatal(err)
	}
	response, err := c.callGroup(ctx, contract.ListGroups{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if gs := response.(contract.ListGroupsResp).Groups; len(gs) != 1 || gs[0].Name != "g-ans" {
		t.Fatalf("list answer %+v", gs)
	}
	response, err = c.callGroup(ctx, contract.DescribeGroups{Groups: []string{"g-ans", "g-none"}})
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if ds := response.(contract.DescribeGroupsResp).Groups; len(ds) != 2 || !ds[0].Exists || ds[1].Exists {
		t.Fatalf("describe answer %+v", ds)
	}
}

// TestCallGroupCanceledContext: a dead context stops the call before it is sent.
func TestCallGroupCanceledContext(t *testing.T) {
	c := realGroupClient(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := c.callGroup(ctx, contract.ListGroups{})
	if err == nil {
		t.Fatal("canceled ctx: expected error")
	}
	if code, _ := (Classifier{}).Classify(runtime.ControlPlaneCall{Op: contract.ListGroups{}}, err); code != runtime.ErrTimeout {
		t.Fatalf("canceled ctx code = %v, want ErrTimeout (err %v)", code, err)
	}
}

// TestCallGroupBusinessErrors: decisions from the group come back as *Error
// with their code, and the answer is nil.
func TestCallGroupBusinessErrors(t *testing.T) {
	c := realGroupClient(t)
	ctx := t.Context()
	j, err := c.JoinGroup(ctx, joinOf("g-biz", ""))
	if err != nil {
		t.Fatal(err)
	}
	// A sync from a member that is not in the group.
	response, err := c.callGroup(ctx, contract.GroupSync{Group: "g-biz", MemberID: "nobody", Generation: j.Generation})
	groupErr(t, err, runtime.ErrUnknownMember)
	if response != nil {
		t.Fatalf("sync resp %+v with error", response)
	}
	// A join that offers another protocol type than the group uses.
	response, err = c.callGroup(ctx, contract.GroupJoin{Group: "g-biz", ClientID: "c", ProtocolType: "other",
		Protocols: []contract.GroupProtocol{{Name: "range", Metadata: []byte("m")}}})
	if err == nil || response != nil {
		t.Fatalf("join with other protocol type: resp %+v err %v", response, err)
	}
}

// TestCallGroupUnknownOp: an op that is not a group op is refused as invalid.
func TestCallGroupUnknownOp(t *testing.T) {
	c := deadClient(t)
	_, err := c.callGroup(t.Context(), contract.CommitSegment{})
	be, ok := errors.AsType[*Error](err)
	if !ok || be.Code != runtime.ErrInvalid {
		t.Fatalf("unknown op: %v", err)
	}
}
