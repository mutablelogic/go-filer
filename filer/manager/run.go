package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	backend "github.com/mutablelogic/go-filer/backend"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	pg "github.com/mutablelogic/go-pg"
	pgqueueschema "github.com/mutablelogic/go-pg/pgqueue/schema"
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

	// Syncronize the volume registry on startup, so that any existing volumes are loaded
	if err := manager.syncVolumes(ctx, logger); err != nil {
		return err
	}

	// Register a ticker to syncronize the volume registry every 5 minutes
	syncVolumesTicker := make(chan json.RawMessage, 100)
	defer close(syncVolumesTicker)
	_, err = manager.queue.RegisterTicker(ctx, "sync-volumes-ticker", pgqueueschema.TickerMeta{
		Interval: types.Ptr(time.Minute * 5),
	}, func(ctx context.Context, payload json.RawMessage) (any, error) {
		syncVolumesTicker <- payload
		return nil, nil
	})

	// Register a ticker to reindex volumes every 5 minutes
	reindexVolumesTicker := make(chan json.RawMessage, 100)
	defer close(reindexVolumesTicker)
	_, err = manager.queue.RegisterTicker(ctx, "reindex-volumes-ticker", pgqueueschema.TickerMeta{
		Interval: types.Ptr(time.Minute * 5),
	}, func(ctx context.Context, payload json.RawMessage) (any, error) {
		reindexVolumesTicker <- payload
		return nil, nil
	})

	// Register a worker to process volume indexing jobs
	indexQueue, err := manager.queue.RegisterQueue(ctx, "index-object", pgqueueschema.QueueMeta{
		TTL:         types.Ptr(time.Duration(15 * time.Minute)),
		Retries:     types.Ptr(uint64(3)),
		RetryDelay:  types.Ptr(time.Minute),
		Concurrency: types.Ptr(uint64(3)),
	}, func(ctx context.Context, payload json.RawMessage) (any, error) {
		var object schema.Object
		if err := json.Unmarshal(payload, &object); err != nil {
			return nil, fmt.Errorf("invalid payload: %w", err)
		}
		logger.DebugContext(ctx, "Index object", "object", types.Stringify(object))
		if err := manager.indexObject(ctx, types.Ptr(object)); err != nil {
			return nil, fmt.Errorf("failed to index object: %w", err)
		}
		return nil, nil
	})
	if err != nil {
		return err
	}

	// Run the queue in the background
	go func() {
		if err := manager.queue.Run(ctx, logger); err != nil {
			logger.ErrorContext(ctx, "queue error", "error", err.Error())
		}
	}()

	// Now start the runloop, which processes all the events
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-volumeChange:
			logger.DebugContext(ctx, "Event", "event", types.Stringify(event))

			// Syncronize the volume registry
			if err := manager.syncVolumes(ctx, logger); err != nil {
				logger.ErrorContext(ctx, "failed to sync volumes", "error", err.Error())
			}
		case <-syncVolumesTicker:
			logger.DebugContext(ctx, "Event", "event", "sync-volumes-ticker")

			// Syncronize the volume registry
			if err := manager.syncVolumes(ctx, logger); err != nil {
				logger.ErrorContext(ctx, "failed to sync volumes", "error", err.Error())
			}
		case <-reindexVolumesTicker:
			logger.DebugContext(ctx, "Event", "event", "reindex-volumes-ticker")

			// Look for a volume that needs to be reindexed, and reindex it
			if err := manager.reindexVolumes(ctx, indexQueue, logger); err != nil {
				logger.ErrorContext(ctx, "failed to reindex volumes", "error", err.Error())
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (manager *Manager) indexObject(ctx context.Context, object *schema.Object) (err error) {
	// Obtain the backend of the object - backend might be disabled, so don't error
	backend := manager.volumes.Get(object.Name)
	if backend == nil {
		return nil
	}

	// Read the object from the backend and extract metadata
	reader, object, err := backend.ReadObject(ctx, schema.GetObjectRequest{
		Path: object.Path,
	})
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, reader.Close())
	}()

	// TODO: Determine if we need to reindex this object

	// Get the metadata for the object
	metadata, err := manager.metadata.Get(ctx, object.ContentType, reader)
	if err != nil {
		return err
	}

	fmt.Println("Metadata for object", object.Path, ":", types.Stringify(metadata))

	// Touch indexed_at for the volume
	var touched schema.Volume
	if err := manager.PoolConn.Update(ctx, &touched, schema.VolumeTouch(backend.Name()), nil); err != nil {
		return err
	}

	// Return success
	return nil
}

func (manager *Manager) reindexVolumes(ctx context.Context, queue *pgqueueschema.Queue, logger *slog.Logger) error {
	var result schema.VolumeList
	if err := manager.List(ctx, &result, &schema.VolumeListRequest{
		Enabled: types.Ptr(true),
		Stale:   true,
		OffsetLimit: pg.OffsetLimit{
			Offset: 0,
			Limit:  types.Ptr(uint64(1)),
		},
	}); err != nil {
		return err
	}
	if len(result.Body) == 0 {
		return nil
	} else if types.Value(result.Body[0].Enabled) == false {
		// Do nothing
	} else if backend := manager.volumes.Get(result.Body[0].Name); backend == nil {
		logger.WarnContext(ctx, "volume not found in registry", "name", result.Body[0].Name)
	} else {
		logger.DebugContext(ctx, "reindexing", "name", backend.Name(), "url", backend.URL().String())

		// Run this in a transaction
		return manager.Tx(ctx, func(conn pg.Conn) error {
			// Touch indexed_at before reindexing so concurrent workers will not pick
			// this volume as stale in the same scheduling window.
			var touched schema.Volume
			if err := conn.Update(ctx, &touched, schema.VolumeTouch(backend.Name()), nil); err != nil {
				return err
			}
			// Create reindexing tasks for all objects in the volume
			return manager.reindexVolumeInner(ctx, backend, queue)
		})
	}

	return nil
}

func (manager *Manager) reindexVolumeInner(ctx context.Context, backend backend.Backend, queue *pgqueueschema.Queue) error {
	// TODO: Do this in a goroutine

	// List all objects in the backend
	var offset uint64
	for {
		if objects, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
			Recursive: true,
			OffsetLimit: pg.OffsetLimit{
				Offset: offset,
			},
		}); err != nil {
			return gofiler.ErrInternalServerError.Withf("backend %q failure: %v", backend.Name(), err.Error())
		} else if len(objects.Body) == 0 {
			break
		} else {
			for _, object := range objects.Body {
				if object.IsDir {
					continue
				}
				payload, err := json.Marshal(object)
				if err != nil {
					return fmt.Errorf("failed to marshal object: %w", err)
				}
				// TODO: Ideally do this in a transaction
				if _, err := manager.queue.CreateTask(ctx, queue.Queue, pgqueueschema.TaskMeta{
					Payload: payload,
				}); err != nil {
					return fmt.Errorf("failed to create index task: %w", err)
				}
			}
			offset += uint64(len(objects.Body))
		}
	}

	// Return success
	return nil
}

func (manager *Manager) syncVolumes(ctx context.Context, logger *slog.Logger) error {
	inserted, deleted, err := manager.syncVolumesInner(ctx)
	if err != nil {
		return err
	}

	// Delete backends
	for _, volume := range deleted {
		if err := manager.volumes.Delete(volume.Name); err != nil {
			logger.ErrorContext(ctx, "failed to unmount volume", "name", volume.Name, "error", err.Error())
			err = errors.Join(err, err)
		} else {
			logger.InfoContext(ctx, "unmounted volume", "name", volume.Name)
		}
	}

	// Insert backends
	for _, volume := range inserted {
		backend, err := manager.volumes.New(volume.URL)
		if err != nil {
			return err
		}
		// Do something with the backend if needed
		logger.InfoContext(ctx, "mounted volume", "name", backend.Name(), "url", backend.URL().String())
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
