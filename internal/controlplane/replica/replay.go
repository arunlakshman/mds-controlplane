package replica

import (
	"errors"
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

// EntryCommands returns the commands that one log entry carries, in the order
// they are applied. A heartbeat or fence entry gives one no-op command. The
// simulator uses it to replay the log into the spec.
func EntryCommands(data []byte) ([]metadata.Entry, error) {
	_, _, payload, ok := unwrap(data)
	if !ok {
		return nil, errors.New("replica: truncated log envelope")
	}
	if _, cmds, ok := decodeGroup(payload); ok {
		return cmds, nil
	}
	e, err := metadata.Decode(payload)
	if err != nil {
		return nil, err
	}
	return []metadata.Entry{e}, nil
}
