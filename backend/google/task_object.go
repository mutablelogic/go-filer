package google

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"path"
	"strconv"
	"strings"

	// Packages
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	ical "github.com/mutablelogic/go-metadata/pkg/ical"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// taskPath is a path of a task filer: the root, a list, or a task of a list
type taskPath struct {
	list string // The list's id, or empty, of the root
	task string // The task's id, or empty, of the root or a list
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// ProdID is the product which writes a task's iCalendar
	ProdID = "-//mutablelogic//go-filer//EN"

	// TaskNamespace is the namespace of the metadata of tasks and lists. A
	// list's folder:children and folder:children-count are schema's.
	TaskNamespace = "task"

	// DCNamespace is the namespace of the Dublin Core metadata, whose
	// "title" is a list's, or a task's
	DCNamespace = "dc"

	// taskExt is the extension of a task's file
	taskExt = ".ics"

	// taskVersion is of a task's entity tag, which is changed when its file,
	// or metadata, are, so a task which hasn't changed, and is indexed, is
	// indexed again, of them, as its revision is of its entity tag
	taskVersion = "/v2"
)

////////////////////////////////////////////////////////////////////////////////
// PATHS

// parseTaskPath returns the path of the root, "/", a list, "/<list>", or a
// task, "/<list>/<task>.ics", which isn't a directory, of a path which
// doesn't end with "/". Returns fs.ErrNotExist of any other path.
func parseTaskPath(op, p string) (taskPath, error) {
	rel := strings.Trim(path.Clean("/"+p), "/")
	if rel == "" {
		return taskPath{}, nil
	}
	parts := strings.Split(rel, "/")
	switch {
	case len(parts) == 1:
		return taskPath{list: parts[0]}, nil
	case len(parts) == 2 && !strings.HasSuffix(p, "/") && strings.HasSuffix(parts[1], taskExt) && len(parts[1]) > len(taskExt):
		return taskPath{list: parts[0], task: strings.TrimSuffix(parts[1], taskExt)}, nil
	default:
		return taskPath{}, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
	}
}

// listPath and taskFilePath return the paths of a list and a task
func listPath(list string) string {
	return "/" + list
}

func taskFilePath(list, task string) string {
	return "/" + list + "/" + task + taskExt
}

////////////////////////////////////////////////////////////////////////////////
// OBJECTS

// rootObject returns the root, which is a directory
func rootObject() *schema.Object {
	return &schema.Object{
		ObjectKey:  schema.ObjectKey{Path: "/"},
		ObjectMeta: schema.ObjectMeta{ContentType: schema.DirectoryType},
		ObjectAttr: schema.ObjectAttr{IsDir: true},
	}
}

// listObject returns a list, which is a directory, with its title, and
// whether it has tasks, and how many, which aren't deleted. Its entity tag
// is the list's, with the count, and its modification time the latest of the
// list's, and its tasks', including those which are deleted, so a change of
// its tasks changes them, as well as its metadata.
func (self *TaskFiler) listObject(list *httpclient.TaskList, tasks []httpclient.Task) *schema.Object {
	count := self.countTasks(tasks)
	modtime := list.Updated
	for i := range tasks {
		if tasks[i].Updated.After(modtime) {
			modtime = tasks[i].Updated
		}
	}
	object := &schema.Object{
		ObjectKey:  schema.ObjectKey{Name: self.name, Path: listPath(list.Id)},
		ObjectMeta: schema.ObjectMeta{ContentType: schema.DirectoryType},
		ObjectAttr: schema.ObjectAttr{IsDir: true, ModTime: modtime},
	}
	if list.ETag != "" {
		object.ETag = types.Ptr(list.ETag + "-" + strconv.Itoa(count))
	}
	object.Meta = appendMeta(object.Meta, "title", list.Title)
	if list.Id != "" && list.Id == self.defaultList {
		object.Meta = append(object.Meta, schema.Meta{Namespace: TaskNamespace, Key: "default", Value: json.RawMessage("true")})
	}
	if list.Title != "" {
		object.Meta = append(object.Meta, schema.Meta{Namespace: DCNamespace, Key: "title", Value: json.RawMessage(quote(list.Title))})
	}
	object.Meta = append(object.Meta, schema.FolderMeta(count)...)
	return object
}

// taskObject returns a task, which is a file, of its iCalendar, with its
// content, and the metadata which the iCalendar can't have, as what it has,
// such as its due date, is go-metadata's, of it, and its title, as dc:title,
// so it has one when it isn't enriched
func (self *TaskFiler) taskObject(list string, task *httpclient.Task) (*schema.Object, []byte) {
	// The calendar, which only fails to be written when the writer does,
	// which a buffer doesn't
	cal := taskCalendar(task)
	content, _ := cal.MarshalText()
	object := &schema.Object{
		ObjectKey:  schema.ObjectKey{Name: self.name, Path: taskFilePath(list, task.Id)},
		ObjectMeta: schema.ObjectMeta{ContentType: cal.ContentType()},
		ObjectAttr: schema.ObjectAttr{Size: types.Ptr(int64(len(content))), ModTime: task.Updated},
	}
	if task.ETag != "" {
		object.ETag = types.Ptr(task.ETag + taskVersion)
	}
	if task.Title != "" {
		object.Meta = append(object.Meta, schema.Meta{Namespace: DCNamespace, Key: "title", Value: json.RawMessage(quote(task.Title))})
	}

	// What the iCalendar can't have: whether it's cleared from the list,
	// and its links, such as to the email it was created from
	if task.Hidden {
		object.Meta = append(object.Meta, schema.Meta{Namespace: TaskNamespace, Key: "hidden", Value: json.RawMessage("true")})
	}
	if len(task.Links) > 0 {
		if data, err := json.Marshal(task.Links); err == nil {
			object.Meta = append(object.Meta, schema.Meta{Namespace: TaskNamespace, Key: "links", Value: data})
		}
	}
	return object, content
}

// appendMeta appends metadata of the task namespace, unless its value is
// empty
func appendMeta(meta []schema.Meta, key, value string) []schema.Meta {
	if value == "" {
		return meta
	}
	return append(meta, schema.Meta{Namespace: TaskNamespace, Key: key, Value: json.RawMessage(quote(value))})
}

////////////////////////////////////////////////////////////////////////////////
// ICALENDAR

// taskCalendar returns a task as an iCalendar, of its VTODO: its id, as its
// UID, its title, notes, due date, status, when it was completed, its
// parent, its link, and when it was updated. Its content type is
// go-metadata's ical.TaskType.
func taskCalendar(task *httpclient.Task) *ical.Calendar {
	todo := &ical.Event{
		Kind:        ical.KindTask,
		UID:         task.Id,
		Summary:     strings.Join(strings.Fields(task.Title), " "),
		Description: strings.TrimSpace(task.Notes),
		Status:      taskStatus(task.Status),
		Parent:      task.Parent,
		URL:         task.WebViewLink,
		Stamp:       task.Updated,
		Modified:    task.Updated,
	}
	if task.Due != nil {
		// A due date, which Google stores as midnight, in UTC
		due := task.Due.UTC()
		todo.Due = ical.Date(due.Year(), due.Month(), due.Day())
	}
	if task.Completed != nil {
		todo.Completed = task.Completed.UTC()
	}
	return &ical.Calendar{ProdID: ProdID, Events: []*ical.Event{todo}}
}

// taskStatus returns the VTODO status of a task's, which is needsAction,
// or completed, or empty, when it's neither
func taskStatus(status string) string {
	switch status {
	case "needsAction":
		return "NEEDS-ACTION"
	case "completed":
		return "COMPLETED"
	default:
		return ""
	}
}

// quote returns a string quoted as JSON, which is a YAML string, without
// escaping <, > and &
func quote(value string) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return `""`
	}
	return strings.TrimSuffix(buf.String(), "\n")
}
