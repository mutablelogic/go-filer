package manager

import (
	"context"
	"fmt"
	"log/slog"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	pg "github.com/mutablelogic/go-pg"
	broadcaster "github.com/mutablelogic/go-pg/pkg/broadcaster"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) Run(ctx context.Context, logger *slog.Logger) error {
	// Create a broadcaster for listening for events
	events, err := broadcaster.NewBroadcaster(manager.PoolConn, schema.NotifyChannel)
	if err != nil {
		return err
	}
	defer events.Close()

	// Subscribe to events and forward them to the volumeChange channel
	volumeChange := make(chan broadcaster.ChangeNotification, 100)
	defer close(volumeChange)
	if err := events.Subscribe(ctx, func(event broadcaster.ChangeNotification) {
		switch event.Table {
		case "volume":
			volumeChange <- event
		default:
			logger.WarnContext(ctx, "ignoring event", "event", types.Stringify(event))
		}
	}); err != nil {
		return err
	}

	// Syncronize the volume registry
	if err := manager.syncVolumes(ctx); err != nil {
		return err
	}

	// Now start the runloop
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-volumeChange:
			// Handle an insert, update or delete for a volume
			logger.InfoContext(ctx, "Event", "event", types.Stringify(event))
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (manager *Manager) syncVolumes(ctx context.Context) error {
	// TODO: Mark the existing volumes

	// List all volumes from the database, and sync them with the registry
	var offset uint64
	for {
		if volumes, err := manager.ListVolumes(ctx, schema.VolumeListRequest{
			OffsetLimit: pg.OffsetLimit{
				Offset: offset,
				Limit:  types.Ptr(uint64(2)),
			},
		}); err != nil {
			return err
		} else if len(volumes.Body) == 0 {
			break
		} else {
			// If volume does not exist, then create it and mark it
			fmt.Println("volumes", types.Stringify(volumes))
			offset += uint64(len(volumes.Body))
		}
	}

	// TODO: Delete the volumes that are not marked

	return nil
}
