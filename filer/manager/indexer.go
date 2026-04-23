package manager

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	queueschema "github.com/mutablelogic/go-filer/queue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	errgroup "golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func (manager *Manager) RunIndexer(ctx context.Context, payload json.RawMessage) (any, error) {
	var object schema.Object
	if err := json.Unmarshal(payload, &object); err != nil {
		return nil, err
	}

	slog.Default().InfoContext(ctx, "Running indexer task", "object", object)
	delay := time.Duration(rand.IntN(100)+1) * time.Second
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(delay):
		return nil, nil
	}
}

func (manager *Manager) QueueIndexTask(ctx context.Context, objects ...schema.Object) error {
	errgroup, errctx := errgroup.WithContext(ctx)

	// Create a task for each object to be indexed. This allows them to be processed in parallel by the indexer.
	for _, obj := range objects {
		errgroup.Go(func() error {
			_, err := manager.queue.CreateTask(errctx, schema.IndexingQueueName, queueschema.TaskMeta{
				Payload: []byte(types.Stringify(obj)),
			})
			return err
		})
	}
	return errgroup.Wait()
}
