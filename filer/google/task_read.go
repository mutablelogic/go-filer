package google

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"slices"
	"strings"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/filer/google/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// taskIterator lists the objects of a listing, in the order of their paths,
// where a directory sorts as its path followed by "/", reading a list's
// tasks when it's reached
type taskIterator struct {
	*TaskFiler
	mu      sync.Mutex
	after   string     // The objects which sort at, or before, it aren't returned
	pending []listItem // The objects, and lists whose tasks are read, in order
	closed  bool
}

// listItem is an object of a listing, or a list whose tasks are read when
// it's reached
type listItem struct {
	object *schema.Object
	list   string               // The list whose tasks are read
	dir    *httpclient.TaskList // The list's directory, which is returned, with the count of its tasks, or nil
	tasks  bool                 // The list's tasks are returned
}

var _ schema.ListObjectsIterator = (*taskIterator)(nil)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// GetObject returns the root, a list, which is a directory, or a task,
// which is a file. Returns ErrNotModified when the request's conditions
// say the object hasn't changed, and fs.ErrNotExist when it doesn't exist.
func (self *TaskFiler) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	object, _, err := self.stat(ctx, "get", req.Path)
	if err != nil {
		return nil, err
	} else if object.NotModified(req) {
		return nil, gofiler.ErrNotModified.Withf("%q", object.Path)
	}
	return object, nil
}

// ReadObject returns the function which reads a task's Markdown, and the
// task. A list, or the root, can't be read, which is fs.ErrInvalid.
func (self *TaskFiler) ReadObject(ctx context.Context, req schema.GetObjectRequest) (gofiler.ReadObjectFunc, *schema.Object, error) {
	object, content, err := self.stat(ctx, "read", req.Path)
	if err != nil {
		return nil, nil, err
	} else if object.IsDir {
		return nil, nil, &fs.PathError{Op: "read", Path: req.Path, Err: fs.ErrInvalid}
	} else if object.NotModified(req) {
		return nil, nil, gofiler.ErrNotModified.Withf("%q", object.Path)
	}
	return func(ctx context.Context, fn func(io.Reader) error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(&contentReader{ctx: ctx, r: bytes.NewReader(content)})
	}, object, nil
}

// ListObjects lists the lists, and, when it's recursive, their tasks, of
// the root, or a list's tasks, or only a task, in the order of their paths.
// The objects which sort at, or before, StartAfter aren't returned. A list
// which doesn't exist is fs.ErrNotExist.
func (self *TaskFiler) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	p, err := parseTaskPath("list", req.Path)
	if err != nil {
		return nil, err
	}
	iter := &taskIterator{TaskFiler: self, after: req.StartAfter}
	switch {
	case p.task != "":
		// Only the task
		object, _, err := self.stat(ctx, "list", req.Path)
		if err != nil {
			return nil, err
		}
		iter.add(object)
	case p.list != "":
		// The list's tasks, once it's known to exist
		if !validId(p.list) {
			return nil, &fs.PathError{Op: "list", Path: req.Path, Err: fs.ErrNotExist}
		} else if _, err := self.client.TaskList(ctx, p.list); err != nil {
			return nil, apiError("list", req.Path, err)
		}
		iter.pending = append(iter.pending, listItem{list: p.list, tasks: true})
	default:
		// The lists, each followed by its tasks, when it's recursive
		lists, err := self.client.TaskLists(ctx)
		if err != nil {
			return nil, apiError("list", "/", err)
		}
		// In the order of their directories' keys, which a page's last is
		// where the next starts after
		slices.SortFunc(lists, func(a, b httpclient.TaskList) int {
			return strings.Compare(listPath(a.Id)+"/", listPath(b.Id)+"/")
		})
		// Each list's tasks are read, when it's reached, for the count of
		// them, of its directory, and its tasks, when it's recursive, unless
		// they all sort before where the listing starts
		for i := range lists {
			if !validId(lists[i].Id) {
				continue
			}
			item := listItem{list: lists[i].Id, tasks: req.Recursive && !iter.before(lists[i].Id)}
			if listPath(lists[i].Id)+"/" > iter.after {
				item.dir = &lists[i]
			}
			if item.dir != nil || item.tasks {
				iter.pending = append(iter.pending, item)
			}
		}
	}
	return iter, nil
}

// Next returns the next object, or io.EOF when there are no more objects
func (self *taskIterator) Next(ctx context.Context) (*schema.Object, error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		} else if self.closed {
			return nil, fs.ErrClosed
		} else if len(self.pending) == 0 {
			return nil, io.EOF
		}
		item := self.pending[0]
		self.pending = self.pending[1:]
		if item.object != nil {
			return item.object, nil
		}
		// Read the list's tasks, which are returned next
		if err := self.readList(ctx, item); err != nil {
			return nil, err
		}
	}
}

// Close the iterator. Any further calls to Next return fs.ErrClosed
func (self *taskIterator) Close() error {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.pending, self.closed = nil, true
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// stat returns the object of a path, and the content of a task
func (self *TaskFiler) stat(ctx context.Context, op, p string) (*schema.Object, []byte, error) {
	path, err := parseTaskPath(op, p)
	if err != nil {
		return nil, nil, err
	}
	switch {
	case path.list == "":
		return rootObject(), nil, nil
	case !validId(path.list) || (path.task != "" && !validId(path.task)):
		return nil, nil, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
	case path.task == "":
		// A list, with the count of its tasks, which are read, as the API
		// doesn't count them
		list, err := self.client.TaskList(ctx, path.list)
		if err != nil {
			return nil, nil, apiError(op, p, err)
		}
		tasks, err := self.client.Tasks(ctx, path.list)
		if err != nil {
			return nil, nil, apiError(op, p, err)
		}
		return self.listObject(list, countTasks(tasks)), nil, nil
	default:
		task, err := self.client.Task(ctx, path.list, path.task)
		if err != nil {
			return nil, nil, apiError(op, p, err)
		} else if task.Deleted {
			return nil, nil, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
		}
		object, content := self.taskObject(path.list, task)
		return object, content, nil
	}
}

// readList reads the tasks of a list, in the order of their ids, to be
// returned next, unless they all sort before where the listing starts,
// which must be locked
func (self *taskIterator) readList(ctx context.Context, item listItem) error {
	list := item.list
	tasks, err := self.client.Tasks(ctx, list)
	if err != nil {
		return apiError("list", listPath(list), err)
	}
	// In the order of their files' paths, which a page's last is where the
	// next starts after
	slices.SortFunc(tasks, func(a, b httpclient.Task) int {
		return strings.Compare(taskFilePath(list, a.Id), taskFilePath(list, b.Id))
	})
	var items []listItem
	if item.dir != nil {
		items = append(items, listItem{object: self.listObject(item.dir, countTasks(tasks))})
	}
	for i := range tasks {
		if item.tasks && !tasks[i].Deleted && validId(tasks[i].Id) {
			if object, _ := self.taskObject(list, &tasks[i]); sortKey(object) > self.after {
				items = append(items, listItem{object: object})
			}
		}
	}
	self.pending = append(items, self.pending...)
	return nil
}

// before returns true when the list's tasks all sort before where the
// listing starts, so they aren't read
func (self *taskIterator) before(list string) bool {
	prefix := listPath(list) + "/"
	return self.after > prefix && !strings.HasPrefix(self.after, prefix)
}

// countTasks returns how many of the tasks are listed, which are those which
// aren't deleted
func countTasks(tasks []httpclient.Task) int {
	var count int
	for i := range tasks {
		if !tasks[i].Deleted && validId(tasks[i].Id) {
			count++
		}
	}
	return count
}

// add adds an object, unless it sorts at, or before, where the listing
// starts
func (self *taskIterator) add(object *schema.Object) {
	if sortKey(object) > self.after {
		self.pending = append(self.pending, listItem{object: object})
	}
}

// contentReader reads a task's content, and seeks, so it can be read in
// ranges, until the context is cancelled, when it returns the context's
// error. It has no other methods, such as WriteTo, which would copy the
// content without the context being checked.
type contentReader struct {
	ctx context.Context
	r   *bytes.Reader
}

var _ io.ReadSeeker = (*contentReader)(nil)

func (c *contentReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

func (c *contentReader) Seek(offset int64, whence int) (int64, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Seek(offset, whence)
}

// sortKey returns the key an object sorts by: its path, followed by "/",
// of a directory
func sortKey(object *schema.Object) string {
	if object.IsDir {
		return object.Path + "/"
	}
	return object.Path
}

// validId returns true when an id can be a name of a path: it isn't empty,
// and has no "/", or ".."
func validId(id string) bool {
	return id != "" && id != "." && id != ".." && !strings.ContainsAny(id, "/\\")
}
