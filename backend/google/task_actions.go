package google

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"strconv"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// taskListAction creates, renames or deletes a task list
type taskListAction struct {
	*TaskFiler
}

// taskListInput is the input of the task_list action
type taskListInput struct {
	Op    string `json:"op" enum:"create,update,delete" help:"What's done: create a list, update (rename) one, or delete one, with its tasks"`
	Path  string `json:"path,omitempty" help:"Path of the list, such as /<id>, to update or delete"`
	Title string `json:"title,omitempty" help:"Title of the list, to create or update"`
}

var _ gofiler.Action = taskListAction{}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// defaultList is Google's alias of a user's default list
	defaultList = "@default"

	opCreate = "create"
	opUpdate = "update"
	opDelete = "delete"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Actions returns what the volume can do, beyond its objects, when it's
// writable: task_list, which creates, renames or deletes a list, and task,
// which creates, updates or deletes a task
func (self *TaskFiler) Actions() []gofiler.Action {
	if self.readonly {
		return nil
	}
	return []gofiler.Action{taskListAction{self}, taskAction{self}}
}

func (taskListAction) Name() string {
	return "task_list"
}

func (taskListAction) Title() string {
	return "Create, rename or delete a task list"
}

func (taskListAction) Description() string {
	return "Creates a task list of its title, or, of the path of a list, such as /<id>, renames it, of its title, or deletes it, with its tasks. Returns the list."
}

func (taskListAction) Input() *jsonschema.Schema {
	return jsonschema.MustFor[taskListInput]()
}

// Run creates a list, or renames, or deletes, the list of the path, and
// returns it, as it is, or was, before it's deleted, which is sent to the
// volume's watches straight away
func (self taskListAction) Run(ctx context.Context, data json.RawMessage) (*schema.Object, error) {
	if self.readonly {
		return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
	}
	var input taskListInput
	if err := decodeInput("task_list", data, &input); err != nil {
		return nil, err
	}
	input.Title = strings.TrimSpace(input.Title)
	switch {
	case input.Op != opCreate && input.Op != opUpdate && input.Op != opDelete:
		return nil, gofiler.ErrBadParameter.Withf("task_list: op is create, update or delete, not %q", input.Op)
	case input.Op != opDelete && input.Title == "":
		return nil, gofiler.ErrBadParameter.Withf("task_list: %s needs a title", input.Op)
	case input.Op == opCreate && input.Path != "":
		return nil, gofiler.ErrBadParameter.With("task_list: create has no path, as Google names the list")
	case input.Op == opDelete && input.Title != "":
		return nil, gofiler.ErrBadParameter.With("task_list: delete has no title")
	case input.Op != opCreate && input.Path == "":
		return nil, gofiler.ErrBadParameter.Withf("task_list: %s needs the path of a list", input.Op)
	}

	// One write at a time, through telling the watches, so they're told in
	// the order Google made them
	self.writes.Lock()
	defer self.writes.Unlock()
	if input.Op == opCreate {
		list, err := self.client.InsertTaskList(ctx, input.Title)
		if err != nil {
			return nil, self.apiError("task_list", "/", err)
		}
		object := self.listObject(list, nil)
		self.changed(taskChange{eventType: schema.EventTypeCreated, path: object.Path, etag: self.listTag(list, nil)})
		return object, nil
	}

	// The list of the path is renamed, or deleted, which must be a list,
	// rather than the root, or a task
	path, err := parseTaskPath("task_list", input.Path)
	if err != nil {
		return nil, err
	} else if path.list == "" || path.task != "" || !googleId(path.list) {
		return nil, &fs.PathError{Op: "task_list", Path: input.Path, Err: fs.ErrNotExist}
	}
	if input.Op == opDelete {
		deleted, err := self.deleteList(ctx, path.list, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: input.Path}, Recursive: true})
		if err != nil {
			return nil, err
		}
		return deleted.Body[len(deleted.Body)-1], nil
	}
	// Its tasks are read first, so a read which fails doesn't leave a
	// rename which isn't sent
	tasks, err := self.client.Tasks(ctx, path.list)
	if err != nil {
		return nil, self.apiError("task_list", input.Path, err)
	}
	list, err := self.client.PatchTaskList(ctx, path.list, input.Title)
	if err != nil {
		return nil, self.apiError("task_list", input.Path, err)
	}
	object := self.listObject(list, tasks)
	self.changed(taskChange{eventType: schema.EventTypeUpdated, path: object.Path, etag: self.listTag(list, tasks)})
	return object, nil
}

////////////////////////////////////////////////////////////////////////////////
// TASK

// taskAction creates, updates or deletes a task
type taskAction struct {
	*TaskFiler
}

// taskInput is the input of the task action. Its fields which are nil
// aren't changed, of an update, and an empty one is cleared.
type taskInput struct {
	Op        string  `json:"op" enum:"create,update,delete" help:"What's done: create a task in a list, update one, or delete one, with its subtasks"`
	Path      string  `json:"path,omitempty" help:"Path of the list to create the task in, such as /<list>, or the default list without it, or of the task to update or delete, such as /<list>/<id>.ics"`
	Title     *string `json:"title,omitempty" help:"Title of the task, which a task which is created has"`
	Notes     *string `json:"notes,omitempty" help:"Notes of the task, or empty to clear them"`
	Due       *string `json:"due,omitempty" help:"Day the task is due, as YYYY-MM-DD, or empty to clear it"`
	Completed *bool   `json:"completed,omitempty" help:"Whether the task is completed"`
	Parent    *string `json:"parent,omitempty" help:"Id of the task of the list which it's a subtask of, or empty for none"`
}

var _ gofiler.Action = taskAction{}

func (taskAction) Name() string {
	return "task"
}

func (taskAction) Title() string {
	return "Create, update or delete a task"
}

func (taskAction) Description() string {
	return "Creates a task in the list of the path, such as /<list>, or in the default list, without a path, of its title, or, of the path of a task, such as /<list>/<id>.ics, updates the fields which are given, or deletes it, with its subtasks. Returns the task."
}

func (taskAction) Input() *jsonschema.Schema {
	return jsonschema.MustFor[taskInput]()
}

// Run creates a task, or updates, or deletes, the task of the path, and
// returns it, as it is, or was, before it's deleted, which is sent to the
// volume's watches straight away
func (self taskAction) Run(ctx context.Context, data json.RawMessage) (*schema.Object, error) {
	if self.readonly {
		return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
	}
	var input taskInput
	if err := decodeInput("task", data, &input); err != nil {
		return nil, err
	}
	path, fields, err := input.check()
	if err != nil {
		return nil, err
	}

	// One write at a time, through telling the watches, so they're told in
	// the order Google made them
	self.writes.Lock()
	defer self.writes.Unlock()
	switch input.Op {
	case opCreate:
		// The default list is got of its id, so the task's path is of it
		if path.list == defaultList {
			list, err := self.client.TaskList(ctx, defaultList)
			if err != nil {
				return nil, self.apiError("task", "/", err)
			}
			path.list = list.Id
		}
		parent := ""
		if input.Parent != nil {
			parent = *input.Parent
			if err := self.checkParent(ctx, path.list, "", parent, input.Path); err != nil {
				return nil, err
			}
		}
		task, err := self.client.InsertTask(ctx, path.list, parent, fields)
		if err != nil {
			return nil, self.apiError("task", input.Path, err)
		}
		object, _ := self.taskObject(path.list, task)
		self.changed(taskChange{eventType: schema.EventTypeCreated, path: object.Path, etag: task.ETag})
		return object, nil
	case opDelete:
		deleted, err := self.deleteTask(ctx, path.list, path.task, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: input.Path}})
		if err != nil {
			return nil, err
		}
		for _, object := range deleted.Body {
			if object.Path == taskFilePath(path.list, path.task) {
				return object, nil
			}
		}
		return nil, &fs.PathError{Op: "task", Path: input.Path, Err: fs.ErrNotExist}
	default:
		return self.update(ctx, path, fields, input)
	}
}

// update changes the fields of a task which are given, and moves it beneath
// its parent, when it's given, and changed, which is checked first, so a
// parent which isn't valid doesn't change it
func (self taskAction) update(ctx context.Context, path taskPath, fields httpclient.TaskFields, input taskInput) (*schema.Object, error) {
	current, err := self.client.Task(ctx, path.list, path.task)
	if err != nil {
		return nil, self.apiError("task", input.Path, err)
	} else if !self.visible(current) {
		return nil, &fs.PathError{Op: "task", Path: input.Path, Err: fs.ErrNotExist}
	}
	move := input.Parent != nil && *input.Parent != current.Parent
	if move {
		if err := self.checkParent(ctx, path.list, path.task, *input.Parent, input.Path); err != nil {
			return nil, err
		}
	}
	task := current
	if len(fields) > 0 {
		if task, err = self.client.PatchTask(ctx, path.list, path.task, fields); err != nil {
			return nil, self.apiError("task", input.Path, err)
		}
	}
	if move {
		moved, err := self.client.MoveTask(ctx, path.list, path.task, *input.Parent)
		if err != nil {
			// The fields are changed, which the watches are sent, though it
			// isn't moved
			if len(fields) > 0 {
				self.changed(taskChange{eventType: schema.EventTypeUpdated, path: taskFilePath(path.list, path.task), etag: task.ETag})
			}
			return nil, errors.Join(gofiler.ErrServiceUnavailable.Withf("task %q is changed, but isn't moved beneath %q", input.Path, *input.Parent), self.apiError("task", input.Path, err))
		}
		task = moved
	}
	object, _ := self.taskObject(path.list, task)
	self.changed(taskChange{eventType: schema.EventTypeUpdated, path: object.Path, etag: task.ETag})
	return object, nil
}

// checkParent returns ErrBadParameter when a parent, which isn't empty,
// isn't another task of the list, which is visible
func (self taskAction) checkParent(ctx context.Context, list, id, parent, p string) error {
	if parent == "" {
		return nil
	} else if !googleId(parent) || parent == id {
		return gofiler.ErrBadParameter.Withf("the task's parent, %q, isn't another task of its list", parent)
	}
	task, err := self.client.Task(ctx, list, parent)
	if errors.Is(self.apiError("task", p, err), fs.ErrNotExist) || (err == nil && !self.visible(task)) {
		return gofiler.ErrBadParameter.Withf("the task's parent, %q, isn't a task of its list", parent)
	} else if err != nil {
		return self.apiError("task", p, err)
	}
	return nil
}

// check returns the path of the input, of a list, to create a task in, or of
// a task, to update or delete, and the fields which are written, of those
// which are given, or ErrBadParameter, when they aren't valid for its op
func (input taskInput) check() (taskPath, httpclient.TaskFields, error) {
	var path taskPath
	if input.Op != opCreate && input.Op != opUpdate && input.Op != opDelete {
		return path, nil, gofiler.ErrBadParameter.Withf("task: op is create, update or delete, not %q", input.Op)
	} else if input.Path == "" && input.Op != opCreate {
		return path, nil, gofiler.ErrBadParameter.Withf("task: %s needs a path", input.Op)
	}

	// A task which is created without a path is of the default list, whose
	// id is got when it's created
	if input.Path == "" {
		path.list = defaultList
	} else if p, err := parseTaskPath("task", input.Path); err != nil {
		return path, nil, err
	} else {
		path = p
	}
	switch {
	case input.Op == opCreate && path.list == defaultList:
	case input.Op == opCreate && (path.list == "" || path.task != ""):
		return path, nil, gofiler.ErrBadParameter.With("task: create's path is of a list, such as /<list>, as Google names the task")
	case input.Op != opCreate && path.task == "":
		return path, nil, gofiler.ErrBadParameter.Withf("task: %s's path is of a task, such as /<list>/<id>%s", input.Op, taskExt)
	case path.list != defaultList && (!googleId(path.list) || (path.task != "" && !googleId(path.task))):
		return path, nil, &fs.PathError{Op: "task", Path: input.Path, Err: fs.ErrNotExist}
	case input.Op == opDelete && (input.Title != nil || input.Notes != nil || input.Due != nil || input.Completed != nil || input.Parent != nil):
		return path, nil, gofiler.ErrBadParameter.With("task: delete has only a path")
	case input.Op == opCreate && (input.Title == nil || strings.TrimSpace(*input.Title) == ""):
		return path, nil, gofiler.ErrBadParameter.With("task: create needs a title")
	case input.Op == opUpdate && input.Title == nil && input.Notes == nil && input.Due == nil && input.Completed == nil && input.Parent == nil:
		return path, nil, gofiler.ErrBadParameter.With("task: update needs a field to change")
	}

	// The fields which are given, of a title which isn't empty, notes and a
	// due date, which are cleared when they're empty, and a status
	fields := httpclient.TaskFields{}
	if input.Title != nil {
		title := strings.Join(strings.Fields(*input.Title), " ")
		if title == "" {
			return path, nil, gofiler.ErrBadParameter.With("task: a title isn't empty")
		}
		fields["title"] = title
	}
	if input.Notes != nil {
		fields["notes"] = strings.TrimSpace(*input.Notes)
	}
	if input.Due != nil {
		if *input.Due == "" {
			fields["due"] = nil
		} else if day, err := time.Parse(time.DateOnly, *input.Due); err != nil {
			return path, nil, gofiler.ErrBadParameter.Withf("task: due is a day, as YYYY-MM-DD, not %q", *input.Due)
		} else {
			fields["due"] = day.UTC().Format(time.RFC3339)
		}
	}
	if input.Completed != nil {
		if *input.Completed {
			fields["status"] = "completed"
		} else {
			fields["status"], fields["completed"] = "needsAction", nil
		}
	}
	return path, fields, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// decodeInput decodes the input of an action, which must be one JSON object,
// without fields it doesn't have, or anything after it
func decodeInput(action string, data json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return gofiler.ErrBadParameter.Withf("%s: %v", action, err)
	} else if _, err := dec.Token(); err != io.EOF {
		return gofiler.ErrBadParameter.Withf("%s: the input is one JSON object, without anything after it", action)
	}
	return nil
}

// listTag returns a list's tag of the watch's snapshot, of its entity tag,
// and the count of its tasks, so a change which is sent isn't sent again
func (self *TaskFiler) listTag(list *httpclient.TaskList, tasks []httpclient.Task) string {
	return list.ETag + "/" + strconv.Itoa(self.countTasks(tasks))
}
