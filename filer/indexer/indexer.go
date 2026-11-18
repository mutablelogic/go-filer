// Package indexer is a filer which wraps another, and keeps an index of its
// folders, in a Store, which it follows the backend's changes in, so its
// folder tree is listed from the index, rather than the backend, which can
// be slow, such as a bucket, or a network mount. Its files are indexed
// later; until then, everything but its folders comes from the backend.
package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"strconv"
	"sync"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Indexer is a filer of a backend, whose folders are indexed, in a store,
// by Run, which scans the backend, and follows its changes. Its folders
// are got, and listed, with ListFolders, from the index, once it's ready.
// Everything else is the backend's, and writes and deletes through it
// update the index.
type Indexer struct {
	gofiler.Filer
	opt
	name  string
	store Store

	// mu serialises changes to the index, of Run, and of writes and
	// deletes, so a folder isn't reconciled with two listings at once
	mu sync.Mutex
}

var _ gofiler.Filer = (*Indexer)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// FolderNamespace is the namespace of a folder's metadata, which the
	// indexer adds, when the backend doesn't: "children", whether it has
	// any objects, and "children-count", how many
	FolderNamespace = "folder"

	// listPage is how many objects of the index are read at a time
	listPage = 1000
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns an indexer of the volume's backend, whose index is in the
// store, which Run builds, and maintains
func New(name string, backend gofiler.Filer, store Store, opts ...Opt) (*Indexer, error) {
	switch {
	case !types.IsIdentifier(name):
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	case backend == nil:
		return nil, gofiler.ErrBadParameter.With("an indexer needs a backend")
	case store == nil:
		return nil, gofiler.ErrBadParameter.With("an indexer needs a store")
	}
	self := &Indexer{Filer: backend, name: name, store: store}
	if err := self.opt.apply(opts); err != nil {
		return nil, err
	}
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Run indexes the backend's folders, and follows its changes, until the
// context is cancelled. It watches the backend, and scans it once it's
// watching, so a change while it scans isn't missed, and scans a folder
// again when the backend says its changes may have been missed. A backend
// which can't be watched is scanned again at the rescan interval. When
// watching fails, it's watched, and scanned, again, after a while, which
// doubles each time, up to a minute.
func (self *Indexer) Run(ctx context.Context) error {
	retry := self.retry
	for {
		err := self.watch(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, gofiler.ErrNotImplemented) {
			return self.poll(ctx)
		}
		self.failed(ctx, err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retry):
		}
		retry = min(retry*2, maxRetry)
	}
}

// State returns the state of the index
func (self *Indexer) State(ctx context.Context) (State, error) {
	return self.store.State(ctx, self.name)
}

// GetObject returns an object, which is a folder of the index, once it's
// ready, or otherwise the backend's
func (self *Indexer) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	if len(req.IfNoneMatch) == 0 && req.IfModifiedSince.IsZero() && self.ready(ctx) {
		if object, err := self.store.Get(ctx, self.name, folderPath(req.Path)); err == nil && object.IsDir {
			object.Name = self.name
			return object, nil
		}
	}
	return self.Filer.GetObject(ctx, req)
}

// ListFolders lists the folders of a folder, or every folder beneath it,
// when the request is recursive, after the path it starts after, in the
// order a backend lists them, from the index, once it's ready, or otherwise
// from the backend
func (self *Indexer) ListFolders(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	parent := folderPath(req.Path)
	if !self.ready(ctx) {
		iter, err := self.Filer.ListObjects(ctx, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Name: req.Name, Path: dirPath(parent)}, Recursive: req.Recursive, StartAfter: req.StartAfter})
		if err != nil {
			return nil, err
		}
		return &folderIterator{backend: iter}, nil
	}
	if _, err := self.store.Get(ctx, self.name, parent); errors.Is(err, gofiler.ErrNotFound) {
		return nil, &fs.PathError{Op: "list", Path: req.Path, Err: fs.ErrNotExist}
	} else if err != nil {
		return nil, err
	}
	after := ""
	if req.StartAfter != "" {
		after = req.StartAfter
	}
	return &folderIterator{indexer: self, req: ListRequest{Parent: parent, Recursive: req.Recursive, After: after, Limit: listPage}}, nil
}

// CreateObject writes an object to the backend, and updates the index, of
// its folder, and of a folder which is created for it
func (self *Indexer) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	object, err := self.Filer.CreateObject(ctx, req)
	if err != nil {
		return nil, err
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	if err := self.refresh(ctx, object.Path, schema.EventTypeCreated); err != nil {
		self.failed(ctx, err)
	}
	return object, nil
}

// DeleteObjects deletes objects of the backend, and updates the index, of
// a folder which is deleted, and of its parent
func (self *Indexer) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	resp, err := self.Filer.DeleteObjects(ctx, req)
	if err != nil {
		return nil, err
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	if err := self.refresh(ctx, folderPath(req.Path), schema.EventTypeDeleted); err != nil {
		self.failed(ctx, err)
	}
	return resp, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS - WATCHING

// watch watches the backend, and scans it, once it's watching, which is its
// first change, a rescan of the root, and follows its changes, until
// watching fails, or the context is cancelled
func (self *Indexer) watch(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	events, done := make(chan schema.Event, 64), make(chan error, 1)
	go func() {
		done <- self.Filer.WatchObjects(ctx, events)
	}()
	for {
		select {
		case err := <-done:
			return err
		case event := <-events:
			if err := self.apply(ctx, event); err != nil {
				return err
			}
		}
	}
}

// poll scans a backend which can't be watched, now, and at the rescan
// interval, until the context is cancelled
func (self *Indexer) poll(ctx context.Context) error {
	for {
		if err := self.scan(ctx, "/"); err != nil && ctx.Err() == nil {
			self.failed(ctx, err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(self.rescanInterval):
		}
	}
}

// apply changes the index, of a change to the backend: a rescan scans the
// folder again, and an object which is created, updated or deleted is
// refreshed, with its folder
func (self *Indexer) apply(ctx context.Context, event schema.Event) error {
	switch event.EventType {
	case schema.EventTypeRescan:
		return self.scan(ctx, folderPath(event.Path))
	case schema.EventTypeCreated, schema.EventTypeUpdated, schema.EventTypeDeleted:
		self.mu.Lock()
		defer self.mu.Unlock()
		return self.refresh(ctx, folderPath(event.Path), event.EventType)
	default:
		return nil
	}
}

// failed records why the index's scan, or watch, failed
func (self *Indexer) failed(ctx context.Context, err error) {
	if state, e := self.store.State(ctx, self.name); e == nil {
		state.Error = err.Error()
		_ = self.store.SetState(ctx, self.name, state)
	}
}

// ready returns true when the index is ready, so folders are read from it
func (self *Indexer) ready(ctx context.Context) bool {
	state, err := self.store.State(ctx, self.name)
	return err == nil && state.Status == StatusReady
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS - SCANNING

// scan indexes the folder, and every folder beneath it, a folder at a time,
// reconciling each folder's folders with the index, so those which aren't
// there any more are removed, with what's beneath them. The root's scan is
// the whole volume's, whose state is scanning, and then ready.
func (self *Indexer) scan(ctx context.Context, root string) error {
	whole := root == "/"
	state, err := self.store.State(ctx, self.name)
	if err != nil {
		return err
	}
	if whole {
		// A volume which is ready is listed from its index while it's
		// scanned again
		if state.Status != StatusReady {
			state.Status = StatusScanning
		}
		state.Folders, state.Started, state.Error = 0, time.Now(), ""
		if err := self.store.SetState(ctx, self.name, state); err != nil {
			return err
		}
	}

	// A folder at a time, from the root
	queue := []string{root}
	for len(queue) > 0 {
		folder := queue[0]
		queue = queue[1:]
		self.mu.Lock()
		folders, err := self.index(ctx, folder)
		self.mu.Unlock()
		if errors.Is(err, fs.ErrNotExist) {
			// It's gone since it was found
			continue
		} else if err != nil {
			return err
		}
		queue = append(queue, folders...)
		if whole {
			state.Folders++
			if err := self.store.SetState(ctx, self.name, state); err != nil {
				return err
			}
		}
	}
	if whole {
		state.Status, state.Scanned = StatusReady, time.Now()
		return self.store.SetState(ctx, self.name, state)
	}
	return nil
}

// index indexes a folder: the folder, of the backend, with the count of its
// objects, and its folders, which replace those the index has, and returns
// their paths. It's called with the indexer locked.
func (self *Indexer) index(ctx context.Context, folder string) ([]string, error) {
	object, err := self.Filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: folder}})
	if err != nil {
		return nil, err
	} else if !object.IsDir {
		// A folder which is now a file isn't a folder of the index
		return nil, errors.Join(self.store.Delete(ctx, self.name, folder), &fs.PathError{Op: "index", Path: folder, Err: fs.ErrNotExist})
	}

	// Its objects, of which its folders are indexed
	iter, err := self.Filer.ListObjects(ctx, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: dirPath(folder)}})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var count int
	folders := make(map[string]*schema.Object)
	for {
		child, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		count++
		if child.IsDir {
			folders[child.Path] = child
		}
	}

	// The folders the index has, which aren't there any more, are removed,
	// with what's beneath them
	indexed, err := self.children(ctx, folder)
	if err != nil {
		return nil, err
	}
	for _, child := range indexed {
		if _, exists := folders[child.Path]; !exists && child.IsDir {
			if err := self.store.Delete(ctx, self.name, child.Path); err != nil {
				return nil, err
			}
		}
	}

	// The folder, with the count of its objects, and its folders, which are
	// replaced with what's listed, keeping those the index has of their own
	// counts until they're indexed
	objects := []*schema.Object{withCount(object, count)}
	result := make([]string, 0, len(folders))
	for p, child := range folders {
		if existing, err := self.store.Get(ctx, self.name, p); err == nil {
			child.Meta = mergeCount(child.Meta, existing.Meta)
		}
		objects = append(objects, child)
		result = append(result, p)
	}
	for _, object := range objects {
		object.Name = ""
	}
	return result, self.store.Put(ctx, self.name, objects...)
}

// refresh changes the index, of an object which is created, updated or
// deleted: a folder which is created is indexed, with the folders beneath
// it, a folder which isn't there any more is removed, and the folder it's
// in is indexed again, as its count of objects, or its folders, changed.
// It's called with the indexer locked.
func (self *Indexer) refresh(ctx context.Context, p string, eventType schema.EventType) error {
	if p != "/" {
		object, err := self.Filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: p}})
		switch {
		case errors.Is(err, fs.ErrNotExist) || (err == nil && !object.IsDir):
			// It isn't a folder, or isn't there any more
			if err := self.store.Delete(ctx, self.name, p); err != nil {
				return err
			}
		case err != nil:
			return err
		case eventType == schema.EventTypeCreated:
			// A folder which is created, which can have folders, such as one
			// which is moved here
			if err := self.subtree(ctx, p); err != nil {
				return err
			}
		}
	}

	// Its folder, and the folders above it, which aren't indexed, such as
	// those created for it
	for parent := Parent(p); parent != ""; parent = Parent(parent) {
		_, err := self.store.Get(ctx, self.name, parent)
		indexed := err == nil
		if _, err := self.index(ctx, parent); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if indexed {
			break
		}
	}
	return nil
}

// subtree indexes a folder, and every folder beneath it. It's called with
// the indexer locked.
func (self *Indexer) subtree(ctx context.Context, root string) error {
	queue := []string{root}
	for len(queue) > 0 {
		folders, err := self.index(ctx, queue[0])
		queue = queue[1:]
		if errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		queue = append(queue, folders...)
	}
	return nil
}

// children returns every object of a folder of the index
func (self *Indexer) children(ctx context.Context, folder string) ([]*schema.Object, error) {
	var result []*schema.Object
	req := ListRequest{Parent: folder, Limit: listPage}
	for {
		page, err := self.store.List(ctx, self.name, req)
		if err != nil {
			return nil, err
		}
		result = append(result, page...)
		if len(page) < listPage {
			return result, nil
		}
		req.After = Key(page[len(page)-1])
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// folderPath returns the path of a folder, without a "/" after it, unless
// it's the root
func folderPath(p string) string {
	for len(p) > 1 && p[len(p)-1] == '/' {
		p = p[:len(p)-1]
	}
	if p == "" {
		return "/"
	}
	return p
}

// dirPath returns the path of a folder which is listed, with a "/" after it
func dirPath(folder string) string {
	if folder == "/" {
		return "/"
	}
	return folder + "/"
}

// withCount returns a folder with the count of its objects, as the
// metadata of the folder namespace, unless the backend has it
func withCount(object *schema.Object, count int) *schema.Object {
	for _, m := range object.Meta {
		if m.Namespace == FolderNamespace && m.Key == "children-count" {
			return object
		}
	}
	object.Meta = append(object.Meta,
		schema.Meta{Namespace: FolderNamespace, Key: "children", Value: json.RawMessage(strconv.FormatBool(count > 0))},
		schema.Meta{Namespace: FolderNamespace, Key: "children-count", Value: json.RawMessage(strconv.Itoa(count))},
	)
	return object
}

// mergeCount returns a folder's metadata, from a listing, with the count of
// its objects which the index has, until it's indexed again
func mergeCount(meta, indexed []schema.Meta) []schema.Meta {
	for _, m := range meta {
		if m.Namespace == FolderNamespace && m.Key == "children-count" {
			return meta
		}
	}
	for _, m := range indexed {
		if m.Namespace == FolderNamespace && (m.Key == "children" || m.Key == "children-count") {
			meta = append(meta, m)
		}
	}
	return meta
}
