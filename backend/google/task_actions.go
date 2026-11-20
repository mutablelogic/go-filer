package google

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	types "github.com/mutablelogic/go-server/pkg/types"
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
	opCreate = "create"
	opUpdate = "update"
	opDelete = "delete"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Actions returns what the volume can do, beyond its objects, when it's
// writable: task_list, which creates, renames or deletes a list
func (self *TaskFiler) Actions() []gofiler.Action {
	if self.readonly {
		return nil
	}
	return []gofiler.Action{taskListAction{self}}
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
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&input); err != nil {
		return nil, gofiler.ErrBadParameter.Withf("task_list: %v", err)
	}
	input.Title = strings.TrimSpace(input.Title)
	switch {
	case input.Op != opCreate && input.Op != opUpdate && input.Op != opDelete:
		return nil, gofiler.ErrBadParameter.Withf("task_list: op is create, update or delete, not %q", input.Op)
	case input.Op != opDelete && input.Title == "":
		return nil, gofiler.ErrBadParameter.Withf("task_list: %s needs a title", input.Op)
	case input.Op == opCreate && input.Path != "":
		return nil, gofiler.ErrBadParameter.With("task_list: create has no path, as Google names the list")
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
		self.changed(taskChange{eventType: schema.EventTypeCreated, path: object.Path, etag: types.Value(object.ETag)})
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
	list, err := self.client.PatchTaskList(ctx, path.list, input.Title)
	if err != nil {
		return nil, self.apiError("task_list", input.Path, err)
	}
	tasks, err := self.client.Tasks(ctx, path.list)
	if err != nil {
		return nil, self.apiError("task_list", input.Path, err)
	}
	object := self.listObject(list, tasks)
	self.changed(taskChange{eventType: schema.EventTypeUpdated, path: object.Path, etag: types.Value(object.ETag)})
	return object, nil
}
