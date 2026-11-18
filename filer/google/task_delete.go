package google

import (
	"context"
	"io/fs"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/filer/google/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// DeleteObjects deletes a task, with its subtasks, as Google does, or a list,
// with its tasks, when the request is recursive, and returns the tasks which
// were deleted, and the list, after them, as a list is an object of its own,
// rather than a directory which is there while it has tasks. The root can't be deleted, which is ErrBadParameter, nor a
// list which isn't recursive, which is ErrConflict, as a directory isn't.
func (self *TaskFiler) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	if self.readonly {
		return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
	}
	path, err := parseTaskPath("delete", req.Path)
	if err != nil {
		return nil, err
	}
	switch {
	case path.list == "":
		return nil, gofiler.ErrBadParameter.Withf("Cannot delete the root of volume %q", self.name)
	case !googleId(path.list) || (path.task != "" && !googleId(path.task)):
		return nil, &fs.PathError{Op: "delete", Path: req.Path, Err: fs.ErrNotExist}
	case path.task == "":
		return self.deleteList(ctx, path.list, req)
	default:
		return self.deleteTask(ctx, path.list, path.task, req)
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// deleteList deletes a list, and its tasks, which the request must be
// recursive for, and returns its tasks, and then the list. A list which
// doesn't exist isn't found, whether the request is recursive or not.
func (self *TaskFiler) deleteList(ctx context.Context, list string, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	meta, err := self.client.TaskList(ctx, list)
	if err != nil {
		return nil, self.apiError("delete", req.Path, err)
	} else if !req.Recursive {
		return nil, gofiler.ErrConflict.Withf("Cannot delete directory %q unless recursive", req.Path)
	}
	tasks, err := self.client.Tasks(ctx, list)
	if err != nil {
		return nil, self.apiError("delete", req.Path, err)
	}
	result := &schema.DeleteObjectsResponse{Body: []*schema.Object{}}
	for i := range tasks {
		if !tasks[i].Deleted && validId(tasks[i].Id) {
			object, _ := self.taskObject(list, &tasks[i])
			result.Body = append(result.Body, object)
		}
	}
	if err := self.client.DeleteTaskList(ctx, list); err != nil {
		return nil, self.apiError("delete", req.Path, err)
	}
	result.Body = append(result.Body, self.listObject(meta, tasks))
	self.deleted(paths(result))
	return result, nil
}

// deleteTask deletes a task, which isn't a directory, so a path which ends
// with "/" isn't one, and its subtasks, which Google deletes with it
func (self *TaskFiler) deleteTask(ctx context.Context, list, id string, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	tasks, err := self.client.Tasks(ctx, list)
	if err != nil {
		return nil, self.apiError("delete", req.Path, err)
	}
	deleted := subtasks(tasks, id)
	if len(deleted) == 0 {
		return nil, &fs.PathError{Op: "delete", Path: req.Path, Err: fs.ErrNotExist}
	}
	if err := self.client.DeleteTask(ctx, list, id); err != nil {
		return nil, self.apiError("delete", req.Path, err)
	}
	result := &schema.DeleteObjectsResponse{Body: make([]*schema.Object, 0, len(deleted))}
	for _, task := range deleted {
		object, _ := self.taskObject(list, task)
		result.Body = append(result.Body, object)
	}
	self.deleted(paths(result))
	return result, nil
}

// paths returns the paths of the objects which were deleted, in their order,
// so a list's tasks are deleted before it
func paths(resp *schema.DeleteObjectsResponse) []string {
	result := make([]string, 0, len(resp.Body))
	for _, object := range resp.Body {
		result = append(result, object.Path)
	}
	return result
}

// subtasks returns the task, which isn't deleted, and those beneath it,
// which aren't, or nil, when there's no such task. The tasks are indexed by
// their parents once, so the subtree is collected in one pass of them.
func subtasks(tasks []httpclient.Task, id string) []*httpclient.Task {
	var root *httpclient.Task
	children := make(map[string][]*httpclient.Task)
	for i := range tasks {
		task := &tasks[i]
		switch {
		case task.Deleted:
			continue
		case task.Id == id:
			root = task
		case task.Parent != "" && validId(task.Id):
			children[task.Parent] = append(children[task.Parent], task)
		}
	}
	if root == nil {
		return nil
	}
	result := []*httpclient.Task{root}
	for i := 0; i < len(result); i++ {
		result = append(result, children[result[i].Id]...)
	}
	return result
}
