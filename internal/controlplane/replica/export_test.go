package replica

import "github.com/manifesterio/mds-controlplane/internal/controlplane/metadata"

// Timer IDs for role and checkpoint tests.
const (
	TimerRole       = timerRole
	TimerCheckpoint = timerCheckpoint
)

// TakeAnswers returns and clears apply-time answers for prior Offers. Only the unit
// tests read answers this way; production gets them as ControlPlaneReply.
func (m *Replica) TakeAnswers() []ReplyToClient {
	out := m.answers
	m.answers = nil
	return out
}

// InFlightCommits is the number of live commit LogAppend ReqIDs (at most one).
func (m *Replica) InFlightCommits() int { return len(m.inFlightAppends) }

// QueueDepth is the number of client ops waiting for a commit slot.
func (m *Replica) QueueDepth() int { return len(m.queue) }

// Booting reports restore or catch-up in progress (not yet Ready).
func (m *Replica) Booting() bool { return m.booting }

// DecodeGroup exports group decoding for tests (strips the machine envelope).
func DecodeGroup(data []byte) (string, []metadata.Entry, bool) {
	return decodeGroup(data)
}

// UnwrapPayload exports envelope stripping for tests.
func UnwrapPayload(data []byte) ([]byte, bool) {
	_, _, payload, ok := unwrap(data)
	return payload, ok
}

// EncodeGroupForTest builds a multi-command entry with a fixed id.
func EncodeGroupForTest(id string, cmds []metadata.Entry) []byte {
	in := make([]metadata.Entry, len(cmds))
	for i, c := range cmds {
		c.ID = id
		in[i] = c
	}
	return encodeGroup(id, in)
}

// EncodeEnvelopedForTest wraps an inner payload with epoch and writer.
func EncodeEnvelopedForTest(epoch uint64, writer string, payload []byte) []byte {
	return wrap(epoch, writer, payload)
}
