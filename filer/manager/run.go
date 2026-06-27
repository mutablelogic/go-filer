package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	backend "github.com/mutablelogic/go-filer/backend"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	llmschema "github.com/mutablelogic/go-llm/kernel/schema"
	pg "github.com/mutablelogic/go-pg"
	pgqueueschema "github.com/mutablelogic/go-pg/pgqueue/schema"
	broadcaster "github.com/mutablelogic/go-pg/pkg/broadcaster"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type indexObjectTask struct {
	schema.ObjectKey
	Force bool `json:"force"`
}

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
	providerChange := make(chan broadcaster.ChangeNotification, 100)
	if err := events.Subscribe(ctx, func(event broadcaster.ChangeNotification) {
		switch event.Table {
		case "volume":
			if manager.indexer {
				volumeChange <- event
			}
		case "llmprovider", "credential":
			providerChange <- event
		default:
			logger.WarnContext(ctx, "ignoring event", "event", types.Stringify(event))
		}
	}); err != nil {
		return err
	}

	// Syncronize the volume registry on startup, so that any existing volumes are loaded
	if manager.indexer {
		if err := manager.syncVolumes(ctx, logger); err != nil {
			return err
		}
	}

	// Syncronize the LLM provider registry whenever a provider or credential change event is received
	logger.DebugContext(ctx, "syncing LLM providers on startup")
	if err := manager.syncLLMProviders(ctx, logger); err != nil {
		return err
	}

	// Register a ticker to syncronize the volume registry every 5 minutes
	syncVolumesTicker := make(chan json.RawMessage, 100)
	_, err = manager.queue.RegisterTicker(ctx, "sync-volumes-ticker", pgqueueschema.TickerMeta{
		Interval: types.Ptr(time.Minute * 5),
	}, func(ctx context.Context, payload json.RawMessage) (any, error) {
		syncVolumesTicker <- payload
		return nil, nil
	})

	// Register a ticker to reindex volumes every 5 minutes
	reindexVolumesTicker := make(chan json.RawMessage, 100)
	_, err = manager.queue.RegisterTicker(ctx, "reindex-volumes-ticker", pgqueueschema.TickerMeta{
		Interval: types.Ptr(time.Minute * 5),
	}, func(ctx context.Context, payload json.RawMessage) (any, error) {
		reindexVolumesTicker <- payload
		return nil, nil
	})

	// Register a worker to process volume indexing jobs
	warnChan := make(chan error, 100)
	indexQueue, err := manager.queue.RegisterQueue(ctx, "index-object", pgqueueschema.QueueMeta{
		TTL:         types.Ptr(time.Duration(5 * time.Minute)),
		Retries:     types.Ptr(uint64(3)),
		RetryDelay:  types.Ptr(time.Minute),
		Concurrency: types.Ptr(uint64(3)),
	}, func(ctx context.Context, payload json.RawMessage) (any, error) {
		// Get the payload
		var task indexObjectTask
		if err := json.Unmarshal(payload, &task); err != nil {
			return nil, gofiler.ErrInternalServerError.Withf("invalid payload: %v", err.Error())
		}

		// Index the object
		logger.DebugContext(ctx, "Index object", "object", types.Stringify(task.ObjectKey), "force", task.Force)
		indexed, err := manager.indexObject(ctx, task.ObjectKey, task.Force)
		if err != nil && indexed == nil {
			return nil, gofiler.ErrInternalServerError.Withf("failed to index object: %v", err.Error())
		} else if err != nil {
			warnChan <- fmt.Errorf("index %q: %w", task.Path, err)
		}
		return indexed, nil
	})
	if err != nil {
		return err
	}
	manager.indexQueue = indexQueue

	// Allow graceful queue drain to cover task TTL plus pgqueue's force-release
	// grace period, with a small buffer for cleanup/logging.
	drainTimeout := types.Value(indexQueue.TTL) + time.Minute + 30*time.Second
	if drainTimeout <= 0 {
		drainTimeout = 90 * time.Second
	}

	// Run the queue in the background; close warnChan once all tasks have finished.
	queueDone := make(chan struct{})
	go func() {
		defer close(queueDone)
		defer close(warnChan)
		if err := manager.queue.Run(ctx, logger); err != nil {
			logger.ErrorContext(ctx, "queue error", "error", err.Error())
		}
	}()

	ctxDone := ctx.Done()
	var shutdownTimer *time.Timer
	var shutdownTimeout <-chan time.Time
	syncVolumesTickerC := syncVolumesTicker
	reindexVolumesTickerC := reindexVolumesTicker
	defer func() {
		if shutdownTimer != nil {
			shutdownTimer.Stop()
		}
	}()

	// Now start the runloop, which processes all the events
	for {
		select {
		case <-ctxDone:
			// Stop scheduling new work; keep looping only to drain warnings until queue shutdown.
			ctxDone = nil
			volumeChange = nil
			providerChange = nil
			syncVolumesTickerC = nil
			reindexVolumesTickerC = nil
			ctx = context.WithoutCancel(ctx)
			shutdownTimer = time.NewTimer(drainTimeout)
			shutdownTimeout = shutdownTimer.C
		case <-queueDone:
			queueDone = nil
		case warn, ok := <-warnChan:
			if !ok {
				// Queue has shut down and all tasks are done.
				return nil
			}
			logger.WarnContext(ctx, "warning", "error", warn.Error())
		case <-shutdownTimeout:
			logger.WarnContext(ctx, "forcing shutdown after timeout while draining queue")
			return nil
		case event := <-providerChange:
			logger.DebugContext(ctx, "LLM provider or credential change", "event", types.Stringify(event))

			// Syncronize the LLM providers
			if err := manager.syncLLMProviders(ctx, logger); err != nil {
				logger.ErrorContext(ctx, "failed to sync LLM providers", "error", err.Error())
			}
		case event := <-volumeChange:
			logger.DebugContext(ctx, "Volume change", "event", types.Stringify(event))

			// Syncronize the volume registry
			if err := manager.syncVolumes(ctx, logger); err != nil {
				logger.ErrorContext(ctx, "failed to sync volumes", "error", err.Error())
			}
		case <-syncVolumesTickerC:
			if manager.indexer {
				logger.DebugContext(ctx, "Sync volumes ticker", "event", "sync-volumes-ticker")

				// Syncronize the volume registry
				if err := manager.syncVolumes(ctx, logger); err != nil {
					logger.ErrorContext(ctx, "failed to sync volumes", "error", err.Error())
				}
			}
		case <-reindexVolumesTickerC:
			logger.DebugContext(ctx, "Reindex volumes ticker", "event", "reindex-volumes-ticker")

			// Look for a volume that needs to be reindexed, and reindex it
			if err := manager.reindexVolumes(ctx, logger); err != nil {
				logger.ErrorContext(ctx, "failed to reindex volumes", "error", err.Error())
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (manager *Manager) indexObject(ctx context.Context, key schema.ObjectKey, force bool) (_ *schema.Object, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "indexObject",
		attribute.String("object", types.Stringify(key)),
		attribute.Bool("force", force),
	)
	defer func() { endSpan(err) }()

	// Obtain the backend of the object - backend might be disabled, so don't error
	backend := manager.volumes.Get(key.Volume)
	if backend == nil {
		return nil, nil
	}

	// Read the object from the backend and extract metadata
	reader, object, err := backend.ReadObject(ctx, schema.GetObjectRequest{
		ObjectKey: key,
	})
	if errors.Is(err, gofiler.ErrNotFound) {
		// Object no longer exists in the backend — remove it from the index
		var deleted schema.Object
		if delErr := manager.Tx(ctx, func(conn pg.Conn) error {
			return conn.Delete(ctx, &deleted, key)
		}); delErr != nil && !errors.Is(delErr, pg.ErrNotFound) {
			return nil, delErr
		}
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, reader.Close())
	}()

	// Unless forced, check if the object has changed and skip re-indexing if not
	if !force {
		existing, err := manager.GetObject(ctx, object.ObjectKey)
		if errors.Is(err, pg.ErrNotFound) {
			// Continue
		} else if err != nil {
			return nil, err
		} else if (existing.ETag != nil && object.ETag != nil && types.Value(existing.ETag) == types.Value(object.ETag)) ||
			(!existing.ModTime.IsZero() && !object.ModTime.IsZero() && existing.ModTime.Truncate(time.Second).Equal(object.ModTime.Truncate(time.Second))) {
			// Object unchanged — touch indexed_at and return the refreshed object
			return manager.touchObject(ctx, object.ObjectKey)
		}
	}

	// Get the metadata for the object - hard error if nothing was extracted, warning otherwise
	metadata, artwork, metaErr := manager.metadata.Get(ctx, object.ContentType, reader)
	if metaErr != nil && metadata == nil {
		return nil, metaErr
	}

	// Create the object in a transaction, and touch the volume's indexed_at
	result, err := manager.createObject(ctx, schema.ObjectCreate{
		ObjectKey:  object.ObjectKey,
		ObjectAttr: object.ObjectAttr,
		ObjectMeta: schema.ObjectMeta{
			ContentType: object.ContentType,
			Meta:        metadata,
		},
	}, artwork)
	if err != nil {
		return nil, err
	}
	return result, metaErr
}

func (manager *Manager) reindexVolumes(ctx context.Context, logger *slog.Logger) error {
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
		// Touch indexed_at before reindexing so concurrent workers will not pick
		// this volume as stale in the same scheduling window.
		var touched schema.Volume
		if err := manager.Update(ctx, &touched, schema.VolumeTouch(backend.Name()), nil); err != nil {
			return err
		}

		// Create reindexing tasks for all objects in the volume
		return manager.reindexVolumeInner(ctx, backend, logger)
	}

	return nil
}

func (manager *Manager) enqueueIndexObject(ctx context.Context, key schema.ObjectKey, force bool) error {
	if manager.indexQueue == nil {
		return gofiler.ErrServiceUnavailable.With("index queue not available")
	}
	payload, err := json.Marshal(indexObjectTask{ObjectKey: key, Force: force})
	if err != nil {
		return fmt.Errorf("failed to marshal index task: %w", err)
	}
	_, err = manager.queue.CreateTask(ctx, manager.indexQueue.Queue, pgqueueschema.TaskMeta{
		Payload: payload,
	})
	return err
}

func (manager *Manager) reindexVolumeInner(ctx context.Context, backend backend.Backend, logger *slog.Logger) error {
	logger.DebugContext(ctx, "reindexing", "name", backend.Name())

	iterator := &schema.ObjectListIterator{
		Recursive: true,
	}
	for {
		err := backend.ListObjects(ctx, iterator)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return gofiler.ErrInternalServerError.Withf("backend %q failure: %v", backend.Name(), err.Error())
		}
		for _, object := range iterator.Body {
			if object.ContentType == schema.ContentTypeDirectory {
				continue
			}
			if err := manager.enqueueIndexObject(ctx, object.ObjectKey, false); err != nil {
				return err
			}
		}
	}

	logger.DebugContext(ctx, "reindexing done", "name", backend.Name())
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
		backend, err := manager.volumes.New(ctx, volume.URL)
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

func (manager *Manager) syncLLMProviders(ctx context.Context, logger *slog.Logger) error {
	var providers []*llmschema.Provider
	updates, deleted, err := manager.llm.Sync(providers, func(i int) (llmschema.ProviderCredentials, error) {
		return llmschema.ProviderCredentials{}, nil
	})
	if err != nil {
		return err
	}

	logger.DebugContext(ctx, "Sync LLM providers", "updates", updates, "deletes", deleted)

	// Return any errors
	return err
}
