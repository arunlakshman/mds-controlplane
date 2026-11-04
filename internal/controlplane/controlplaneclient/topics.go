package controlplaneclient

import (
	"context"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Topic calls: CommitSegment, Lookup, DescribeTopics and CreateTopic. The server
// side is server/topics.go.

// callTopic sends one topics operation.
func (c *Client) callTopic(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneResponse, error) {
	switch operation := operation.(type) {
	case contract.CommitSegment:
		return c.commitSegment(ctx, operation)
	case contract.FindSegments:
		return c.lookup(ctx, operation)
	case contract.DescribeTopics:
		return c.describeTopics(ctx, operation)
	case contract.CreateTopic:
		return nil, c.createTopic(ctx, operation)
	}
	return nil, &Error{Code: runtime.ErrInvalid}
}

func (c *Client) commitSegment(ctx context.Context, operation contract.CommitSegment) (runtime.ControlPlaneResponse, error) {
	return callOp(ctx, c, "CommitSegment", cpv1.ControlPlaneClient.CommitSegment,
		wireboundary.CommitSegmentFromCore(operation), wireboundary.CommitSegmentRespToCore)
}

func (c *Client) lookup(ctx context.Context, operation contract.FindSegments) (runtime.ControlPlaneResponse, error) {
	return callOp(ctx, c, "Lookup", cpv1.ControlPlaneClient.Lookup,
		wireboundary.LookupFromCore(operation), wireboundary.LookupRespToCore)
}

func (c *Client) describeTopics(ctx context.Context, operation contract.DescribeTopics) (runtime.ControlPlaneResponse, error) {
	return callOp(ctx, c, "DescribeTopics", cpv1.ControlPlaneClient.DescribeTopics,
		wireboundary.DescribeTopicsFromCore(operation), wireboundary.DescribeTopicsRespToCore)
}

// CreateTopic is the dev/test helper over CreateTopic.
func (c *Client) CreateTopic(ctx context.Context, name string, partitions int32) error {
	return c.createTopic(ctx, contract.CreateTopic{Name: name, Partitions: partitions})
}

// createTopic sends op. FailIfExists makes the create exclusive.
func (c *Client) createTopic(ctx context.Context, operation contract.CreateTopic) error {
	return c.do(ctx, "CreateTopic", func(ctx context.Context, cli cpv1.ControlPlaneClient) error {
		_, err := cli.CreateTopic(ctx, &cpv1.CreateTopicRequest{
			Topic: operation.Name, Partitions: operation.Partitions, FailIfExists: operation.FailIfExists,
		})
		return err
	})
}
