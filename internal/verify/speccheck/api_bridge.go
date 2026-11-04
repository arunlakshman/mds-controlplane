package speccheck

import (
	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/wireboundary"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// ExecRequest runs a generated CommitSegment/FindSegments/DescribeTopics request
// through the oracle. Apply logic stays in this package (decision 21); only
// the message types are shared with the wire API.
func (c *ControlPlane) ExecRequest(request runtime.RequestID, msg any) runtime.ControlPlaneDone {
	switch m := msg.(type) {
	case *cpv1.CommitSegmentRequest:
		return c.Exec(runtime.ControlPlaneCall{Req: request, Op: wireboundary.CommitSegmentToCore(m)}).(runtime.ControlPlaneDone)
	case *cpv1.LookupRequest:
		return c.Exec(runtime.ControlPlaneCall{Req: request, Op: wireboundary.LookupToCore(m)}).(runtime.ControlPlaneDone)
	case *cpv1.DescribeTopicsRequest:
		return c.Exec(runtime.ControlPlaneCall{Req: request, Op: wireboundary.DescribeTopicsToCore(m)}).(runtime.ControlPlaneDone)
	case *cpv1.CreateTopicRequest:
		code := c.CreateTopic(m.GetTopic(), m.GetPartitions())
		return runtime.ControlPlaneDone{Req: request, Err: code}
	default:
		return runtime.ControlPlaneDone{Req: request, Err: runtime.ErrInvalid}
	}
}
