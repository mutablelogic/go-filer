package google

import (
	"bytes"
	"context"
	"errors"
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
	types "github.com/mutablelogic/go-server/pkg/types"
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
	case !googleId(path.list) || (path.task != NewTask && !googleId(path.task)):
		return nil, &fs.PathError{Op: "create", Path: req.Path, Err: fs.ErrNotExist}
	}
	if req.ContentType != "" {
		if mediaType, _, err := mime.ParseMediaType(req.ContentType); err != nil || (mediaType != ical.ContentType && mediaType != types.ContentTypeBinary) {
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
		err = self.apiError("create", req.Path, err)
	} else if !self.visible(current) {
		err = &fs.PathError{Op: "create", Path: req.Path, Err: fs.ErrNotExist}
	}
	switch {
	case errors.Is(err, fs.ErrNotExist) && req.IfExists:
		return nil, gofiler.ErrPreconditionFailed.Withf("task %q doesn't exist", req.Path)
	case err != nil:
		return nil, err
	case req.IfNotExists:
		return nil, gofiler.ErrConflict.Withf("task %q exists", req.Path)
	case todo.UID != "" && todo.UID != id:
		return nil, gofiler.ErrBadParameter.Withf("the task's UID, %q, isn't its id, %q", todo.UID, id)
	}

	// A parent which changes must be a task of the list, which is checked
	// before the task is changed, so a bad parent doesn't change it
	move := todo.Parent != current.Parent
	if move && todo.Parent != "" {
		if !googleId(todo.Parent) || todo.Parent == id {
			return nil, gofiler.ErrBadParameter.Withf("the task's parent, %q, isn't another task of its list", todo.Parent)
		} else if parent, err := self.client.Task(ctx, list, todo.Parent); errors.Is(self.apiError("create", req.Path, err), fs.ErrNotExist) || (err == nil && !self.visible(parent)) {
			return nil, gofiler.ErrBadParameter.Withf("the task's parent, %q, isn't a task of its list", todo.Parent)
		} else if err != nil {
			return nil, self.apiError("create", req.Path, err)
		}
	}
	task, err := self.client.PatchTask(ctx, list, id, fields)
	if err != nil {
		return nil, self.apiError("create", req.Path, err)
	}
	if move {
		moved, err := self.client.MoveTask(ctx, list, id, todo.Parent)
		if err != nil {
			// The fields are changed, which the watches are sent, though it
			// isn't moved
			self.changed(taskChange{eventType: schema.EventTypeUpdated, path: taskFilePath(list, id), etag: task.ETag})
			return nil, errors.Join(gofiler.ErrServiceUnavailable.Withf("task %q is changed, but isn't moved beneath %q", req.Path, todo.Parent), self.apiError("create", req.Path, err))
		}
		task = moved
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
	props, err := taskProperties(data)
	if err != nil {
		return nil, err
	}
	cal, err := ical.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("a task's iCalendar: %v", err)
	} else if len(cal.Events) != 1 || cal.Events[0].Kind != ical.KindTask {
		return nil, gofiler.ErrBadParameter.With("a task is written as an iCalendar, of one VTODO")
	}

	// A date which it has, which isn't one, isn't taken as none, which would
	// clear it
	todo := cal.Events[0]
	for name, zero := range map[string]bool{"DUE": todo.Due.IsZero(), "COMPLETED": todo.Completed.IsZero()} {
		if value, exists := props[name]; exists && zero {
			return nil, gofiler.ErrBadParameter.Withf("the task's %s, %q, isn't a date, or a time", name, value)
		}
	}
	return todo, nil
}

// taskProperties returns the properties of the VTODO of an iCalendar, by
// their names, which must be the only VTODO, without any events, journals
// or free/busy times, though it can have its zones, and its VTODO alarms,
// as they're read, rather than decoded, which ignores what it can't read
func taskProperties(data []byte) (map[string]string, error) {
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	text = strings.NewReplacer("\n ", "", "\n\t", "").Replace(text)
	var stack []string
	props := make(map[string]string)
	todos := 0
	for _, line := range strings.Split(text, "\n") {
		if line = strings.TrimRight(line, "\r"); line == "" {
			continue
		}
		name, value, _ := strings.Cut(line, ":")
		if i := strings.Index(name, ";"); i >= 0 {
			name = name[:i]
		}
		name, value = strings.ToUpper(strings.TrimSpace(name)), strings.TrimSpace(value)
		switch name {
		case "BEGIN":
			component := strings.ToUpper(value)
			switch {
			case len(stack) == 0 && component != "VCALENDAR":
				return nil, gofiler.ErrBadParameter.With("a task is written as an iCalendar, of one VTODO")
			case component == "VTODO" && len(stack) == 1:
				todos++
			case component == "VEVENT", component == "VJOURNAL", component == "VFREEBUSY":
				return nil, gofiler.ErrBadParameter.Withf("a task is written as an iCalendar, of one VTODO, without a %s", component)
			}
			stack = append(stack, component)
		case "END":
			if len(stack) == 0 || stack[len(stack)-1] != strings.ToUpper(value) {
				return nil, gofiler.ErrBadParameter.Withf("a task's iCalendar has an END:%s without its BEGIN", value)
			}
			stack = stack[:len(stack)-1]
		default:
			if len(stack) == 2 && stack[1] == "VTODO" {
				props[name] = value
			}
		}
	}
	if todos != 1 || len(stack) != 0 {
		return nil, gofiler.ErrBadParameter.With("a task is written as an iCalendar, of one VTODO")
	}
	return props, nil
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
