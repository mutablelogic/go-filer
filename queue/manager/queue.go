package manager

import (
	"context"
	"errors"

	// Packages
	schema "github.com/mutablelogic/go-filer/queue/schema"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterQueue creates a new queue, or updates an existing queue, and returns it.
func (manager *Manager) RegisterQueue(ctx context.Context, name string, meta schema.QueueMeta, callback schema.TaskFunc) (*schema.Queue, error) {
	var queue schema.Queue

	// Register task callback if provided
	if err := manager.queues.RegisterTask(name, callback); err != nil {
		return nil, err
	}

	err := manager.Tx(ctx, func(conn pg.Conn) error {
		err := conn.Get(ctx, &queue, schema.QueueName(name))
		switch {
		case err == nil:
			// Queue already exists, optionally patch below.
		case errors.Is(err, pg.ErrNotFound):
			if err := conn.Insert(ctx, &queue, schema.Queue{Queue: name}); err != nil {
				return err
			}
		default:
			return err
		}

		if !hasQueueMetaPatch(meta) {
			return nil
		}

		return conn.Update(ctx, &queue, schema.QueueName(name), schema.Queue{Queue: name, QueueMeta: meta})
	})
	if err != nil {
		return nil, errors.Join(err, manager.queues.RemoveTask(name))
	}

	// Return success
	return types.Ptr(queue), nil
}

// ListQueues returns all queues as a list.
func (manager *Manager) ListQueues(ctx context.Context, req schema.QueueListRequest) (*schema.QueueList, error) {
	result := schema.QueueList{QueueListRequest: req}
	if err := manager.List(ctx, &result, req); err != nil {
		return nil, err
	} else {
		result.OffsetLimit.Clamp(result.Count)
	}
	return types.Ptr(result), nil
}

// GetQueue returns a queue by name.
func (manager *Manager) GetQueue(ctx context.Context, name string) (*schema.Queue, error) {
	var queue schema.Queue
	if err := manager.Get(ctx, &queue, schema.QueueName(name)); err != nil {
		return nil, err
	}
	return types.Ptr(queue), nil
}

// DeleteQueue deletes an existing queue, and returns it.
func (manager *Manager) DeleteQueue(ctx context.Context, name string) (*schema.Queue, error) {
	var queue schema.Queue
	if err := manager.Delete(ctx, &queue, schema.QueueName(name)); err != nil {
		return nil, err
	} else if err := manager.queues.RemoveTask(name); err != nil {
		return nil, err
	}
	return types.Ptr(queue), nil
}

// UpdateQueue updates an existing queue, and returns it.
func (manager *Manager) UpdateQueue(ctx context.Context, name string, meta schema.QueueMeta) (*schema.Queue, error) {
	var queue schema.Queue
	if err := manager.Update(ctx, &queue, schema.QueueName(name), schema.Queue{Queue: name, QueueMeta: meta}); err != nil {
		return nil, err
	}
	return types.Ptr(queue), nil
}

// CleanQueue removes stale tasks from a queue, and returns the tasks removed.
func (manager *Manager) CleanQueue(ctx context.Context, name string) ([]schema.Task, error) {
	var resp schema.QueueCleanResponse
	if err := manager.List(ctx, &resp, schema.QueueCleanRequest{Queue: name}); err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// ListQueueStatuses returns the status breakdown for all queues.
func (manager *Manager) ListQueueStatuses(ctx context.Context) ([]schema.QueueStatus, error) {
	var resp schema.QueueStatusResponse
	if err := manager.List(ctx, &resp, schema.QueueStatusRequest{}); err != nil {
		return nil, err
	}
	return resp.Body, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func hasQueueMetaPatch(meta schema.QueueMeta) bool {
	return meta.TTL != nil || meta.Retries != nil || meta.RetryDelay != nil || meta.Concurrency != nil
}
