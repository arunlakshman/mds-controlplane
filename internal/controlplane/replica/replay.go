package replica

import (
	"fmt"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadata"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// ReplayPosition folds one durable log entry into fold. Used for the reference
// replay of the Dynamo log. Client waiters and InjectedBugWriterOffsets stamps are
// ignored: truth assigns offsets at apply time like a cold replica.
func ReplayPosition(meta *metadata.Store, slot runtime.LogPosition, data []byte) error {
	_, _, payload, ok := unwrap(data)
	if !ok {
		return fmt.Errorf("replica: truncated log envelope at slot %d", slot)
	}
	if _, cmds, ok := decodeGroup(payload); ok {
		for _, c := range cmds {
			c.WriterBases = nil
			meta.Apply(int64(slot), c)
		}
		return nil
	}
	_, err := meta.ApplyBytes(int64(slot), payload)
	return err
}
