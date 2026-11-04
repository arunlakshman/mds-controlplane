package controlplanetest

import (
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/server"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Node is a started gRPC control-plane node.
type Node struct {
	Addr   string
	Client *Client
	API    cpv1.ControlPlaneClient
	stop   func()
}

// StartMem boots a rank-0 machine over an in-memory log.
func StartMem(t testing.TB) *Node {
	t.Helper()
	return Start(t, replica.Config{
		CatchUpEvery: runtime.Duration(time.Millisecond),
		Rank:         0, Heartbeat: runtime.Duration(time.Second),
	}, executor.NewMemoryLog(), executor.NewMemoryObjects())
}

// Start boots a node with the given machine config and stores.
func Start(t testing.TB, config replica.Config, log executor.Log, objs executor.ObjectStore) *Node {
	t.Helper()
	n := server.New(server.Config{
		Machine: config, Log: log, Objects: objs,
		Self: runtime.NodeInstance{Node: "cp-0"},
	})
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitWriter(t, n, config)
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	n.Register(gs)
	go func() { _ = gs.Serve(ln) }()
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	cli := NewClient(ln.Addr().String())
	stop := func() {
		_ = cli.Close()
		_ = conn.Close()
		gs.GracefulStop()
		_ = n.Close()
	}
	t.Cleanup(stop)
	return &Node{
		Addr: ln.Addr().String(), Client: cli,
		API: cpv1.NewControlPlaneClient(conn), stop: stop,
	}
}

// waitWriter waits until a rank 0 node with roles is the active writer. Start
// returns when the node is Ready, but a node commits only when it is also active
// (it has claimed the epoch and applied its fence entry), and a write in that
// gap is refused with ErrUnavailable (real clients retry it). A test that writes
// right after Start must not see that gap. A node that is not rank 0, or has no
// roles, never waits.
func waitWriter(t testing.TB, n *server.Node, config replica.Config) {
	t.Helper()
	if config.Rank != 0 || (config.Heartbeat <= 0 && config.TakeoverAfter <= 0) {
		return
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if st, err := n.GetStatus(t.Context(), &cpv1.GetStatusRequest{}); err == nil && st.GetRole() == "active" {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("controlplanetest: the node did not become the active writer within 10s")
}
