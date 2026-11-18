package google

import (
	"context"
	"io"
	"io/fs"
	"mime"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/filer/google/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	ical "github.com/mutablelogic/go-metadata/pkg/ical"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// NewTask is the name of the file, /<list>/~new.ics, which is written to
	// create a task, as Google gives a task its id, which can't have "~",
	// so it's never a task's
	NewTask = "~new"

	// maxTaskSize is the largest iCalendar of a task which is read
	maxTaskSize = 1 << 20
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// CreateObject writes a task, of an iCalendar of one VTODO. Writing
// /<list>/~new.ics creates a task, which is returned at its path, of the id
// Google gives it, and writing a task's path replaces the fields a VTODO
// has: its title, notes, due date and status, which are cleared when it
// doesn't have them, and its parent, when it's changed, which moves it. Its
// other fields, such as its links, are kept. A task which doesn't exist
// isn't found, as a task's id can't be chosen.
func (self *TaskFiler) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	if self.readonly {
		return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
	}
	path, err := parseTaskPath("create", req.Path)
	if err != nil {
		return nil, err
	}
	switch {
	case path.task == "":
		return nil, gofiler.ErrBadParameter.Withf("only a task can be written, as /<list>/<task>%s, or /<list>/%s%s, which creates one", taskExt, NewTask, taskExt)
	case !validId(path.list) || !validId(path.task):
		return nil, &fs.PathError{Op: "create", Path: req.Path, Err: fs.ErrNotExist}
	}
	if req.ContentType != "" {
		if mediaType, _, err := mime.ParseMediaType(req.ContentType); err != nil || mediaType != ical.ContentType {
			return nil, gofiler.ErrBadParameter.Withf("a task is written as %s, not %q", ical.ContentType, req.ContentType)
		}
	}
	todo, err := readTask(req.Body)
	if err != nil {
		return nil, err
	}
	fields, err := taskFields(todo)
	if err != nil {
		return nil, err
	}

	// Create, or update, the task
	var task *httpclient.Task
	eventType := schema.EventTypeUpdated
	if path.task == NewTask {
		if req.IfExists {
			return nil, gofiler.ErrPreconditionFailed.Withf("%q creates a task, which doesn't exist", req.Path)
		}
		if task, err = self.client.InsertTask(ctx, path.list, todo.Parent, fields); err != nil {
			return nil, self.apiError("create", req.Path, err)
		}
		eventType = schema.EventTypeCreated
	} else if task, err = self.updateTask(ctx, path.list, path.task, todo, fields, req); err != nil {
		return nil, err
	}

	// Its object, which the watches are sent
	object, _ := self.taskObject(path.list, task)
	self.changed(taskChange{eventType: eventType, path: object.Path, etag: task.ETag})
	return object, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// updateTask replaces the fields of a task, which must exist, and isn't
// written when the request is only to create it, and moves it beneath its
// new parent, when it's changed
func (self *TaskFiler) updateTask(ctx context.Context, list, id string, todo *ical.Event, fields httpclient.TaskFields, req schema.CreateObjectRequest) (*httpclient.Task, error) {
	current, err := self.client.Task(ctx, list, id)
	if err != nil {
		return nil, self.apiError("create", req.Path, err)
	} else if current.Deleted {
		return nil, &fs.PathError{Op: "create", Path: req.Path, Err: fs.ErrNotExist}
	} else if req.IfNotExists {
		return nil, gofiler.ErrConflict.Withf("task %q exists", req.Path)
	} else if todo.UID != "" && todo.UID != id {
		return nil, gofiler.ErrBadParameter.Withf("the task's UID, %q, isn't its id, %q", todo.UID, id)
	}
	task, err := self.client.PatchTask(ctx, list, id, fields)
	if err != nil {
		return nil, self.apiError("create", req.Path, err)
	}
	if todo.Parent != current.Parent {
		if task, err = self.client.MoveTask(ctx, list, id, todo.Parent); err != nil {
			return nil, self.apiError("create", req.Path, err)
		}
	}
	return task, nil
}

// readTask returns the VTODO of an iCalendar, which must be the only one,
// without any events, and is at most maxTaskSize
func readTask(r io.Reader) (*ical.Event, error) {
	if r == nil {
		return nil, gofiler.ErrBadParameter.With("a task is written as an iCalendar, of one VTODO")
	}
	data, err := io.ReadAll(io.LimitReader(r, maxTaskSize+1))
	if err != nil {
		return nil, err
	} else if len(data) > maxTaskSize {
		return nil, gofiler.ErrBadParameter.Withf("a task's iCalendar is larger than %d bytes", maxTaskSize)
	}
	cal, err := ical.Decode(strings.NewReader(string(data)))
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("a task's iCalendar: %v", err)
	} else if len(cal.Events) != 1 || cal.Events[0].Kind != ical.KindTask {
		return nil, gofiler.ErrBadParameter.With("a task is written as an iCalendar, of one VTODO")
	}
	return cal.Events[0], nil
}

// taskFields returns the fields of a task, of its VTODO, which replace
// those it has, so a field it doesn't have is cleared: its title, notes, a
// due date, which is its day, as Google's has no time, and its status,
// which is completed, or needs action, which clears when it was completed
func taskFields(todo *ical.Event) (httpclient.TaskFields, error) {
	fields := httpclient.TaskFields{
		"title": strings.Join(strings.Fields(todo.Summary), " "),
		"notes": strings.TrimSpace(todo.Description),
		"due":   nil,
	}
	if !todo.Due.IsZero() {
		year, month, day := todo.Due.Time.Date()
		fields["due"] = time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	}
	switch strings.ToUpper(todo.Status) {
	case "", "NEEDS-ACTION", "IN-PROCESS":
		fields["status"], fields["completed"] = "needsAction", nil
	case "COMPLETED":
		fields["status"] = "completed"
		if !todo.Completed.IsZero() {
			fields["completed"] = todo.Completed.UTC().Format(time.RFC3339)
		}
	default:
		return nil, gofiler.ErrBadParameter.Withf("a task's status is NEEDS-ACTION, IN-PROCESS or COMPLETED, not %q", todo.Status)
	}
	return fields, nil
}
