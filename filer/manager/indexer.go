package manager

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	queueschema "github.com/mutablelogic/go-filer/queue/schema"
	pg "github.com/mutablelogic/go-pg"
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

	// Get the object and any error message
	actual_object, err := manager.GetObject(ctx, object.Name, schema.GetObjectRequest{
		Path: object.Path,
	})

	// Insert or delete the object, based on the remote state.
	if err := manager.PoolConn.Tx(ctx, func(conn pg.Conn) error {
		if errors.Is(err, filer.ErrNotFound) {
			return conn.Delete(ctx, nil, schema.ObjectKey{
				Name: object.Name,
				Path: object.Path,
			})
		} else if err != nil {
			return err
		} else {
			return conn.Insert(ctx, &object, actual_object)
		}
	}); err != nil {
		return nil, err
	}

	// Return success
	return nil, nil
}

func (manager *Manager) QueueIndexTask(ctx context.Context, objects ...schema.Object) error {
	errgroup, errctx := errgroup.WithContext(ctx)

	// Create a task for each object to be indexed. This allows them to be processed in parallel by the indexer.
	for _, obj := range objects {
		obj := obj // capture current loop iteration value
		errgroup.Go(func() error {
			_, err := manager.queue.CreateTask(errctx, schema.IndexingQueueName, queueschema.TaskMeta{
				DelayedAt: types.Ptr(time.Now().Add(10 * time.Second)), // delay slightly to allow for any concurrent updates to complete before indexing
				Payload:   []byte(types.Stringify(obj)),
			})
			return err
		})
	}
	return errgroup.Wait()
}
