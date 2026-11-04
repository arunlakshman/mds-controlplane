package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/server"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func newClosingNode() *server.Node {
	return server.New(server.Config{
		Machine: replica.Config{CatchUpEvery: runtime.Duration(time.Millisecond)},
		Log:     executor.NewMemoryLog(), Objects: executor.NewMemoryObjects(),
		Self: runtime.NodeInstance{Node: "cp-0"},
	})
}

// closeWithin fails the test if Close does not return soon.
func closeWithin(t *testing.T, n *server.Node) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- n.Close() }()
	select {
	case err := <-done:
		t.Logf("Close returned: %v", err) // a stop error is fine; it must only return
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked")
	}
}

// Start waits for the loop to end when it gives up, so the loop result was
// already taken. Close used to wait for it a second time and block for ever.
func TestCloseAfterFailedStartReturns(t *testing.T) {
	n := newClosingNode()
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Start gives up at once
	if err := n.Start(ctx); err == nil {
		t.Fatal("Start with an ended context must fail")
	}
	closeWithin(t, n)
	closeWithin(t, n) // and again: safe to repeat
}

func TestCloseWithoutStartReturns(t *testing.T) {
	closeWithin(t, newClosingNode())
}

func TestCloseAfterStartStopsTheLoop(t *testing.T) {
	n := newClosingNode()
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	closeWithin(t, n)
}
