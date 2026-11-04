package controlplanetest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Client is a small test client over the generated gRPC client (api/v1). The
// target is a comma separated list of addresses: a call goes to the address
// that worked last, and moves on to the next one when a node is unavailable.
// It is for tests only; the real brokers have their own client.
type Client struct {
	addrs []string
	cur   atomic.Int32
	mu    sync.Mutex // guards conns: tests call the client from several goroutines
	conns []*grpc.ClientConn
}

// Error is a business error answered by the control plane.
type Error struct{ Code runtime.ErrCode }

func (e *Error) Error() string { return "cp: " + e.Code.String() }

// CallError is a failed exchange (dial or transport). Sent says whether the
// request may have reached the server.
type CallError struct {
	Op   string
	Sent bool
	Code codes.Code
	Err  error
}

func (e *CallError) Error() string { return fmt.Sprintf("cp: %s: grpc %s: %v", e.Op, e.Code, e.Err) }
func (e *CallError) Unwrap() error { return e.Err }

// NewClient returns a client for the addresses in target.
func NewClient(target string) *Client {
	c := &Client{}
	for a := range strings.SplitSeq(target, ",") {
		if a = strings.TrimSpace(a); a != "" {
			c.addrs = append(c.addrs, a)
		}
	}
	c.conns = make([]*grpc.ClientConn, len(c.addrs))
	return c
}

// Close closes all connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	for i, conn := range c.conns {
		if conn != nil {
			err = errors.Join(err, conn.Close())
			c.conns[i] = nil
		}
	}
	return err
}

func (c *Client) api(i int) (cpv1.ControlPlaneClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conns[i] == nil {
		conn, err := grpc.NewClient(c.addrs[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.conns[i] = conn
	}
	return cpv1.NewControlPlaneClient(c.conns[i]), nil
}

// do runs fn against the preferred address and tries the others when a node
// is unavailable.
func (c *Client) do(ctx context.Context, name string, fn func(cpv1.ControlPlaneClient) error) error {
	n := len(c.addrs)
	if n == 0 {
		return &CallError{Op: name, Err: errors.New("no addresses")}
	}
	start := int(c.cur.Load())
	var err error
	for i := range n {
		idx := (start + i) % n
		cli, derr := c.api(idx)
		if derr != nil {
			err = &CallError{Op: name, Err: derr}
		} else if err = convert(name, fn(cli)); err == nil {
			c.cur.Store(int32(idx))
			return nil
		}
		if ctx.Err() != nil || !tryNext(err) {
			return err
		}
	}
	return err
}

func tryNext(err error) bool {
	if ce, ok := errors.AsType[*CallError](err); ok {
		return !ce.Sent || ce.Code == codes.Unavailable
	}
	be, ok := errors.AsType[*Error](err)
	return ok && be.Code == runtime.ErrUnavailable
}

// convert turns a gRPC error into *Error (a business answer) or *CallError.
func convert(name string, err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return &CallError{Op: name, Sent: true, Err: err}
	}
	for _, d := range st.Details() {
		if ed, ok := d.(*cpv1.ErrorDetail); ok {
			return &Error{Code: wireboundary.ToCoreErr(ed.Code)}
		}
	}
	switch st.Code() {
	case codes.Unavailable:
		return &Error{Code: runtime.ErrUnavailable}
	case codes.InvalidArgument:
		return &Error{Code: runtime.ErrInvalid}
	case codes.NotFound:
		return &Error{Code: runtime.ErrNotFound}
	case codes.AlreadyExists:
		return &Error{Code: runtime.ErrConflict}
	}
	return &CallError{Op: name, Sent: true, Code: st.Code(), Err: err}
}

// rpc runs one RPC with failover and converts the answer with toCore.
func rpc[Req, Resp any](ctx context.Context, c *Client, name string,
	call func(cpv1.ControlPlaneClient, context.Context, Req, ...grpc.CallOption) (Resp, error),
	in Req, toCore func(Resp) runtime.ControlPlaneResponse,
) (runtime.ControlPlaneResponse, error) {
	var out runtime.ControlPlaneResponse
	err := c.do(ctx, name, func(cli cpv1.ControlPlaneClient) error {
		r, e := call(cli, ctx, in)
		if e == nil {
			out = toCore(r)
		}
		return e
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Call performs one topics operation.
func (c *Client) Call(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneResponse, error) {
	switch op := operation.(type) {
	case contract.CommitSegment:
		return rpc(ctx, c, "CommitSegment", cpv1.ControlPlaneClient.CommitSegment, wireboundary.CommitSegmentFromCore(op),
			func(r *cpv1.CommitSegmentResponse) runtime.ControlPlaneResponse {
				return wireboundary.CommitSegmentRespToCore(r)
			})
	case contract.FindSegments:
		return rpc(ctx, c, "Lookup", cpv1.ControlPlaneClient.Lookup, wireboundary.LookupFromCore(op),
			func(r *cpv1.LookupResponse) runtime.ControlPlaneResponse { return wireboundary.LookupRespToCore(r) })
	case contract.DescribeTopics:
		return rpc(ctx, c, "DescribeTopics", cpv1.ControlPlaneClient.DescribeTopics, wireboundary.DescribeTopicsFromCore(op),
			func(r *cpv1.DescribeTopicsResponse) runtime.ControlPlaneResponse {
				return wireboundary.DescribeTopicsRespToCore(r)
			})
	case contract.CreateTopic:
		return rpc(ctx, c, "CreateTopic", cpv1.ControlPlaneClient.CreateTopic,
			&cpv1.CreateTopicRequest{Topic: op.Name, Partitions: op.Partitions, FailIfExists: op.FailIfExists},
			func(*cpv1.CreateTopicResponse) runtime.ControlPlaneResponse { return nil })
	}
	return nil, &Error{Code: runtime.ErrInvalid}
}

// CreateTopic creates a topic.
func (c *Client) CreateTopic(ctx context.Context, name string, partitions int32) error {
	_, err := c.Call(ctx, contract.CreateTopic{Name: name, Partitions: partitions})
	return err
}

// GetStatus calls GetStatus.
func (c *Client) GetStatus(ctx context.Context) (*cpv1.GetStatusResponse, error) {
	var out *cpv1.GetStatusResponse
	err := c.do(ctx, "GetStatus", func(cli cpv1.ControlPlaneClient) error {
		var e error
		out, e = cli.GetStatus(ctx, &cpv1.GetStatusRequest{})
		return e
	})
	return out, err
}
