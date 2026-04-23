package manager

import (
	"context"
	"encoding/json"
	"errors"

	// Packages
	schema "github.com/mutablelogic/go-filer/queue/schema"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - TASK

// CreateTask creates a new task in a queue, and returns it.
func (manager *Manager) CreateTask(ctx context.Context, queue string, meta schema.TaskMeta) (*schema.Task, error) {
	var taskId schema.TaskId
	var task schema.TaskWithStatus

	if err := manager.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.With("id", queue).Insert(ctx, &taskId, meta); err != nil {
			return err
		}
		return conn.Get(ctx, &task, taskId)
	}); err != nil {
		return nil, err
	}

	return types.Ptr(task.Task), nil
}

// ListTasks returns all tasks with optional queue and status filtering.
func (manager *Manager) ListTasks(ctx context.Context, req schema.TaskListRequest) (*schema.TaskList, error) {
	result := schema.TaskList{TaskListRequest: req}
	if err := manager.List(ctx, &result, req); err != nil {
		return nil, err
	} else {
		result.OffsetLimit.Clamp(result.Count)
	}
	return types.Ptr(result), nil
}

// NextTask retains a task from any of the specified queues, and returns it.
// If no task is available, nil is returned.
func (manager *Manager) NextTask(ctx context.Context, worker string, queues ...string) (*schema.Task, error) {
	var taskId schema.TaskId
	var task schema.TaskWithStatus

	if err := manager.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.Get(ctx, &taskId, schema.TaskRetain{Queues: queues, Worker: worker}); errors.Is(err, pg.ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		if taskId == 0 {
			return nil
		}

		return conn.Get(ctx, &task, taskId)
	}); err != nil {
		return nil, err
	}

	if taskId == 0 {
		return nil, nil
	}

	return types.Ptr(task.Task), nil
}

// ReleaseTask releases a task, optionally marking it as failed, and returns it.
func (manager *Manager) ReleaseTask(ctx context.Context, taskId uint64, success bool, result json.RawMessage, status *string) (*schema.Task, error) {
	var released schema.TaskId
	var task schema.TaskWithStatus

	if err := manager.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.Get(ctx, &released, schema.TaskRelease{Id: taskId, Fail: !success, Result: result}); err != nil {
			return err
		}

		if released == 0 {
			return pg.ErrNotFound
		}

		return conn.Get(ctx, &task, released)
	}); err != nil {
		return nil, err
	}

	if status != nil {
		*status = task.Status
	}

	return types.Ptr(task.Task), nil
}
