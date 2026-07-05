package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	// Packages

	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	llmschema "github.com/mutablelogic/go-llm/kernel/schema"
	pg "github.com/mutablelogic/go-pg"
	pgqueueschema "github.com/mutablelogic/go-pg/pgqueue/schema"
	broadcaster "github.com/mutablelogic/go-pg/pkg/broadcaster"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type indexObjectTask struct {
	schema.ObjectKey
	Force bool `json:"force"`
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (filer *Filer) Run(ctx context.Context, logger *slog.Logger) (err error) {
	// If the context is cancelled while starting up (before the runloop's own
	// graceful shutdown handling takes over), don't report that as a failure.
	defer func() {
		if err != nil && ctx.Err() != nil {
			err = nil
		}
	}()

	// Create a broadcaster for listening for events
	events, err := broadcaster.NewBroadcaster(filer.PoolConn, schema.NotifyChannel)
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
			if filer.indexer {
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
	if err := filer.syncVolumes(ctx, logger); err != nil {
		logger.WarnContext(ctx, "failed to sync volumes", "error", err.Error())
	}

	// Syncronize the LLM provider registry whenever a provider or credential change event is received
	logger.DebugContext(ctx, "syncing LLM providers on startup")
	if err := filer.syncLLMProviders(ctx, logger); err != nil {
		return err
	}

	// Register a ticker to syncronize the volume registry every 5 minutes
	syncVolumesTicker := make(chan json.RawMessage, 100)
	_, err = filer.queue.RegisterTicker(ctx, "sync-volumes-ticker", pgqueueschema.TickerMeta{
		Interval: types.Ptr(time.Minute * 5),
	}, func(ctx context.Context, payload json.RawMessage) (any, error) {
		syncVolumesTicker <- payload
		return nil, nil
	})

	// Register a ticker to reindex volumes every 5 minutes
	reindexVolumesTicker := make(chan json.RawMessage, 100)
	_, err = filer.queue.RegisterTicker(ctx, "reindex-volumes-ticker", pgqueueschema.TickerMeta{
		Interval: types.Ptr(time.Minute * 5),
	}, func(ctx context.Context, payload json.RawMessage) (any, error) {
		reindexVolumesTicker <- payload
		return nil, nil
	})

	// Register a worker to process volume indexing jobs
	warnChan := make(chan error, 100)
	indexQueue, err := filer.queue.RegisterQueue(ctx, "index-object", pgqueueschema.QueueMeta{
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
		indexed, err := filer.indexObject(ctx, task.ObjectKey, task.Force)
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
	filer.indexQueue = indexQueue

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
		if err := filer.queue.Run(ctx, logger); err != nil {
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

	// Start the credential manager runloop in the background
	var wg sync.WaitGroup
	defer wg.Wait() // Wait for all background goroutines to finish before returning
	wg.Go(func() {
		if err := filer.Credentials.Run(ctx, logger); err != nil {
			logger.ErrorContext(ctx, "credential manager error", "error", err.Error())
		}
	})

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
			if err := filer.syncLLMProviders(ctx, logger); err != nil {
				logger.ErrorContext(ctx, "failed to sync LLM providers", "error", err.Error())
			}
		case event := <-volumeChange:
			logger.DebugContext(ctx, "Volume change", "event", types.Stringify(event))

			// Syncronize the volume registry
			if err := filer.syncVolumes(ctx, logger); err != nil {
				logger.ErrorContext(ctx, "failed to sync volumes", "error", err.Error())
			}
		case <-syncVolumesTickerC:
			if err := filer.syncVolumes(ctx, logger); err != nil {
				logger.ErrorContext(ctx, "failed to sync volumes", "error", err.Error())
			}
		case <-reindexVolumesTickerC:
			logger.DebugContext(ctx, "Reindex volumes ticker", "event", "reindex-volumes-ticker")

			// Look for a volume that needs to be reindexed, and reindex it
			if err := filer.reindexVolumes(ctx, logger); err != nil {
				logger.ErrorContext(ctx, "failed to reindex volumes", "error", err.Error())
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (filer *Filer) reindexVolumes(ctx context.Context, logger *slog.Logger) error {
	var result schema.VolumeList
	if err := filer.List(ctx, &result, &schema.VolumeListRequest{
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
	}

	backend := filer.volumes.Get(result.Body[0].Name)
	if backend == nil {
		logger.WarnContext(ctx, "volume not found in registry", "name", result.Body[0].Name)
		return nil
	}

	// Touch indexed_at before reindexing so concurrent workers will not pick
	// this volume as stale in the same scheduling window.
	var touched schema.Volume
	if err := filer.Update(ctx, &touched, schema.VolumeTouch(backend.Name()), nil); err != nil {
		return err
	}

	// Create reindexing tasks for all objects in the volume - don't force if any objects are already
	// indexed
	return filer.ReindexVolume(ctx, backend.Name(), schema.ObjectListFilters{
		Recursive: true,
	}, false, func(offset uint64) {
		logger.DebugContext(ctx, "Reindexing volume", "name", backend.Name(), "offset", offset)
	})
}

func (filer *Filer) syncVolumes(ctx context.Context, logger *slog.Logger) error {
	var result error

	inserted, deleted, err := filer.syncVolumesInner(ctx)
	if err != nil {
		return err
	}

	// Delete backends
	for _, volume := range deleted {
		if err := filer.volumes.Delete(volume.Name); err != nil {
			result = errors.Join(result, fmt.Errorf("failed to unmount volume %q: %v", volume.Name, err))
		} else {
			logger.InfoContext(ctx, "unmounted volume", "name", volume.Name)
		}
	}

	// Insert backends
	for _, volume := range inserted {
		backend, err := filer.volumes.New(ctx, volume.URL)
		if err != nil {
			result = errors.Join(result, fmt.Errorf("failed to mount volume %q: %v", volume.Name, err))
		} else {
			logger.InfoContext(ctx, "mounted volume", "name", backend.Name(), "url", backend.URL().String())
		}
	}

	// Return any errors
	return result
}

func (filer *Filer) syncVolumesInner(ctx context.Context) (inserted []*schema.Volume, deleted []*schema.Volume, err error) {
	// Get a list of names
	names := make(map[string]struct{})
	for _, name := range filer.volumes.Names() {
		names[name] = struct{}{}
	}

	// List all volumes from the database, and sync them with the registry
	var offset uint64
	for {
		if volumes, err := filer.ListVolumes(ctx, schema.VolumeListRequest{
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

func (filer *Filer) syncLLMProviders(ctx context.Context, logger *slog.Logger) error {
	var providers []*llmschema.Provider
	updates, deleted, err := filer.llm.Sync(providers, func(i int) (llmschema.ProviderCredentials, error) {
		return llmschema.ProviderCredentials{}, nil
	})
	if err != nil {
		return err
	}

	logger.DebugContext(ctx, "Sync LLM providers", "updates", updates, "deletes", deleted)

	// Return any errors
	return err
}
