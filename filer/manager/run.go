package manager

import (
	"context"
	"errors"
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
	if err := manager.syncVolumes(ctx, logger); err != nil {
		return err
	}

	// Now start the runloop
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-volumeChange:
			// Handle an insert, update or delete for a volume
			logger.DebugContext(ctx, "Event", "event", types.Stringify(event))

			// Syncronize the volume registry
			if err := manager.syncVolumes(ctx, logger); err != nil {
				return err
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (manager *Manager) syncVolumes(ctx context.Context, logger *slog.Logger) error {
	inserted, deleted, err := manager.syncVolumesInner(ctx)
	if err != nil {
		return err
	}

	// Delete backends
	for _, volume := range deleted {
		if err := manager.volumes.Delete(volume.Name); err != nil {
			logger.ErrorContext(ctx, "failed to delete volume", "name", volume.Name, "error", err)
			err = errors.Join(err, err)
		} else {
			logger.InfoContext(ctx, "deleted volume", "name", volume.Name)
		}
	}

	// Insert backends
	for _, volume := range inserted {
		backend, err := manager.volumes.New(volume.URL)
		if err != nil {
			return err
		}
		// Do something with the backend if needed
		logger.InfoContext(ctx, "inserted volume", "name", backend.Name(), "url", backend.URL().String())
	}

	// Return any errors
	return err
}

func (manager *Manager) syncVolumesInner(ctx context.Context) (inserted []*schema.Volume, deleted []*schema.Volume, err error) {
	// Get a list of names
	names := make(map[string]struct{})
	for _, name := range manager.volumes.Names() {
		names[name] = struct{}{}
	}

	// List all volumes from the database, and sync them with the registry
	var offset uint64
	for {
		if volumes, err := manager.ListVolumes(ctx, schema.VolumeListRequest{
			OffsetLimit: pg.OffsetLimit{
				Offset: offset,
				Limit:  types.Ptr(uint64(2)),
			},
		}); err != nil {
			return nil, nil, err
		} else if len(volumes.Body) == 0 {
			break
		} else {
			for _, volume := range volumes.Body {
				if _, exists := names[volume.Name]; exists {
					if !types.Value(volume.Enabled) {
						// Only delete disabled volumes that are also present in the registry.
						deleted = append(deleted, volume)
					}
					// Mark volume as seen so any remaining registry-only entries can be deleted.
					delete(names, volume.Name)
				} else if types.Value(volume.Enabled) {
					// Enabled volumes missing from the registry need inserting.
					inserted = append(inserted, volume)
				}
			}

			offset += uint64(len(volumes.Body))
		}
	}

	// Add any volumes in the registry that were not 'seen' as deleted
	for name := range names {
		deleted = append(deleted, &schema.Volume{Name: name})
	}

	return inserted, deleted, nil
}
