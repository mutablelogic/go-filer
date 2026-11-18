// Package indexer is a filer which wraps another, and keeps an index of its
// folders, in a Store, which it follows the backend's changes in, so its
// folder tree is listed from the index, rather than the backend, which can
// be slow, such as a bucket, or a network mount. Its files are indexed
// later; until then, everything but its folders comes from the backend.
package indexer

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"strings"
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

	// The calls to WatchObjects, which are sent the backend's changes, once
	// they're indexed, so the backend is watched once
	smu      sync.Mutex
	watchers map[*watcher]struct{}
}

// watcher is a call to WatchObjects, which is sent changes, until it's
// cancelled, or the backend's watch fails, when it's sent why
type watcher struct {
	ctx    context.Context
	ch     chan<- schema.Event
	failed chan error
}

var _ gofiler.Filer = (*Indexer)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// listPage is how many objects of the index are read at a time
	listPage = 1000

	// progressEvery is how many folders a scan lists between storing its
	// progress
	progressEvery = 100
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
		self.fail(err)
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

// Backend returns the filer which is indexed
func (self *Indexer) Backend() gofiler.Filer {
	return self.Filer
}

// URL returns the backend's URL, with index=folders, as its folders are
// indexed
func (self *Indexer) URL() *url.URL {
	u := self.Filer.URL()
	query := u.Query()
	query.Set(IndexParam, string(ModeFolders))
	u.RawQuery = query.Encode()
	return u
}

// Clone returns an indexer of a clone of the backend, with the patch, of
// the same store, and options, which scans it, when it's run, as an
// indexer of a backend which is opened again does
func (self *Indexer) Clone(ctx context.Context, patch url.Values) (gofiler.Filer, error) {
	backend, err := self.Filer.Clone(ctx, patch)
	if err != nil {
		return nil, err
	}
	return &Indexer{Filer: backend, opt: self.opt, name: self.name, store: self.store}, nil
}

// Readonly returns true when the backend says its objects can't be written
func (self *Indexer) Readonly() bool {
	if b, ok := self.Filer.(interface{ Readonly() bool }); ok {
		return b.Readonly()
	}
	return false
}

// Close closes the backend, when it holds resources, which its owner, such
// as the registry, calls, once Run has returned
func (self *Indexer) Close() error {
	if closer, ok := self.Filer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// WatchObjects sends the backend's changes, once they're indexed, so the
// backend is watched once, by Run, until the context is cancelled, or the
// backend's watch fails, which is its error, so the caller can open it
// again. The first change is a rescan of the root, and a backend which
// can't be watched sends one each time it's scanned again.
func (self *Indexer) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	w := &watcher{ctx: ctx, ch: ch, failed: make(chan error, 1)}
	self.smu.Lock()
	if self.watchers == nil {
		self.watchers = make(map[*watcher]struct{})
	}
	self.watchers[w] = struct{}{}
	self.smu.Unlock()
	defer func() {
		self.smu.Lock()
		delete(self.watchers, w)
		self.smu.Unlock()
	}()
	select {
	case ch <- schema.Rescan(self.name):
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-w.failed:
		return err
	}
}

// GetObject returns an object, which is a folder of the index, once it's
// ready, when the path is a folder's, which ends with "/", or is the root,
// or otherwise the backend's, which resolves a path without one, which can
// be a file, and a folder, of a bucket
func (self *Indexer) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	isFolder := strings.HasSuffix(req.Path, "/") || folderPath(req.Path) == "/"
	if isFolder && len(req.IfNoneMatch) == 0 && req.IfModifiedSince.IsZero() && self.ready(ctx) {
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
			self.forward(event)
		}
	}
}

// poll scans a backend which can't be watched, now, and at the rescan
// interval, until the context is cancelled
func (self *Indexer) poll(ctx context.Context) error {
	for {
		if err := self.scan(ctx, "/"); err != nil && ctx.Err() == nil {
			self.failed(ctx, err)
		} else if err == nil {
			// Its watchers compare what they know with it, as its changes
			// aren't known
			self.forward(schema.Rescan(self.name))
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

// forward sends a change, which is indexed, to the watchers
func (self *Indexer) forward(event schema.Event) {
	self.smu.Lock()
	watchers := make([]*watcher, 0, len(self.watchers))
	for w := range self.watchers {
		watchers = append(watchers, w)
	}
	self.smu.Unlock()
	for _, w := range watchers {
		select {
		case w.ch <- event:
		case <-w.ctx.Done():
		}
	}
}

// fail tells the watchers the backend's watch failed, with why
func (self *Indexer) fail(err error) {
	self.smu.Lock()
	defer self.smu.Unlock()
	for w := range self.watchers {
		select {
		case w.failed <- err:
		default:
		}
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

// tree is a folder's subtree, of the backend: the folder, and the folders
// beneath it, by their paths, with how many objects each lists
type tree struct {
	root    *schema.Object
	folders map[string]*schema.Object
	counts  map[string]int
}

// scan indexes the folder, and every folder beneath it, of one recursive
// listing, so the backend decides which folders are descended into, as it
// does for its own, such as a filesystem's link to a directory, which
// isn't. The index's folders beneath it, which aren't there any more, are
// removed. The root's scan is the whole volume's, whose state is scanning,
// and then ready.
func (self *Indexer) scan(ctx context.Context, root string) error {
	whole := root == "/"
	state, err := self.store.State(ctx, self.name)
	if err != nil {
		return err
	}
	var progress func(int)
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
		progress = func(folders int) {
			state.Folders = uint64(folders)
			_ = self.store.SetState(ctx, self.name, state)
		}
	}

	// List it, then change the index, which is locked only while it's
	// changed
	t, err := self.list(ctx, root, progress)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	self.mu.Lock()
	err = self.replace(ctx, root, t)
	self.mu.Unlock()
	if err != nil {
		return err
	}
	if whole {
		state.Status, state.Folders, state.Scanned = StatusReady, uint64(len(t.folders)+1), time.Now()
		return self.store.SetState(ctx, self.name, state)
	}
	return nil
}

// list returns a folder's subtree, of one recursive listing of the backend,
// reporting how many folders it's listed every so often, or fs.ErrNotExist,
// with an empty subtree, when the folder isn't there
func (self *Indexer) list(ctx context.Context, root string, progress func(int)) (tree, error) {
	t := tree{folders: make(map[string]*schema.Object), counts: map[string]int{root: 0}}
	object, err := self.Filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: dirPath(root)}})
	if err != nil {
		return t, err
	} else if !object.IsDir {
		return t, &fs.PathError{Op: "index", Path: root, Err: fs.ErrNotExist}
	}
	t.root = object
	iter, err := self.Filer.ListObjects(ctx, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: dirPath(root)}, Recursive: true})
	if err != nil {
		return t, err
	}
	defer iter.Close()
	for {
		child, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			return t, nil
		} else if err != nil {
			return t, err
		}
		t.counts[Parent(child.Path)]++
		if child.IsDir {
			t.folders[child.Path] = child
			if _, exists := t.counts[child.Path]; !exists {
				t.counts[child.Path] = 0
			}
			if progress != nil && len(t.folders)%progressEvery == 0 {
				progress(len(t.folders))
			}
		}
	}
}

// replace replaces the index's subtree of a folder with the backend's,
// removing the folders which aren't there any more, with what's beneath
// them, or the whole subtree, when the folder isn't there. It's called with
// the indexer locked.
func (self *Indexer) replace(ctx context.Context, root string, t tree) error {
	if t.root == nil {
		return self.store.Delete(ctx, self.name, root)
	}
	indexed, err := self.beneath(ctx, root)
	if err != nil {
		return err
	}
	for _, object := range indexed {
		if _, exists := t.folders[object.Path]; !exists && object.IsDir {
			if err := self.store.Delete(ctx, self.name, object.Path); err != nil {
				return err
			}
		}
	}
	objects := make([]*schema.Object, 0, len(t.folders)+1)
	t.root.Path = root
	objects = append(objects, withCount(t.root, t.counts[root]))
	for p, folder := range t.folders {
		objects = append(objects, withCount(folder, t.counts[p]))
	}
	for _, object := range objects {
		object.Name = ""
	}
	return self.store.Put(ctx, self.name, objects...)
}

// index indexes a folder again, which isn't descended into: the folder,
// with how many objects it lists, and its folders, which replace those the
// index has. A folder which isn't there any more is removed, with what's
// beneath it, which is fs.ErrNotExist. It's called with the indexer locked.
func (self *Indexer) index(ctx context.Context, folder string) error {
	object, err := self.Filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: dirPath(folder)}})
	if errors.Is(err, fs.ErrNotExist) || (err == nil && !object.IsDir) {
		if err := self.store.Delete(ctx, self.name, folder); err != nil {
			return err
		}
		return &fs.PathError{Op: "index", Path: folder, Err: fs.ErrNotExist}
	} else if err != nil {
		return err
	}

	// Its objects, of which its folders are indexed
	iter, err := self.Filer.ListObjects(ctx, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: dirPath(folder)}})
	if err != nil {
		return err
	}
	defer iter.Close()
	count := 0
	folders := make(map[string]*schema.Object)
	for {
		child, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		count++
		if child.IsDir {
			folders[child.Path] = child
		}
	}

	// Its folders which aren't there any more are removed, and those which
	// are keep the counts the index has of them, until they're indexed
	indexed, err := self.children(ctx, folder)
	if err != nil {
		return err
	}
	counts := make(map[string][]schema.Meta)
	for _, child := range indexed {
		if _, exists := folders[child.Path]; !exists && child.IsDir {
			if err := self.store.Delete(ctx, self.name, child.Path); err != nil {
				return err
			}
		} else {
			counts[child.Path] = child.Meta
		}
	}
	object.Path = folder
	objects := []*schema.Object{withCount(object, count)}
	for p, child := range folders {
		child.Meta = mergeCount(child.Meta, counts[p])
		objects = append(objects, child)
	}
	for _, object := range objects {
		object.Name = ""
	}
	return self.store.Put(ctx, self.name, objects...)
}

// refresh changes the index, of an object which is created, updated or
// deleted, of its folder's path, which is a folder, of the backend, or
// isn't: a folder which is there, and is created, or isn't indexed, is
// indexed, with the folders beneath it, and a folder which isn't there any
// more is removed. Then the folders above it are indexed again, as their
// counts, or folders, changed, up to the first which is there, and was
// indexed, removing those which aren't there any more. It's called with the
// indexer locked.
func (self *Indexer) refresh(ctx context.Context, p string, eventType schema.EventType) error {
	if p != "/" {
		object, err := self.Filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: dirPath(p)}})
		_, indexErr := self.store.Get(ctx, self.name, p)
		switch {
		case errors.Is(err, fs.ErrNotExist) || (err == nil && !object.IsDir):
			// It isn't a folder, or isn't there any more
			if err := self.store.Delete(ctx, self.name, p); err != nil {
				return err
			}
		case err != nil:
			return err
		case eventType == schema.EventTypeCreated || errors.Is(indexErr, gofiler.ErrNotFound):
			// A folder which is created, which can have folders, such as one
			// which is moved here
			t, err := self.list(ctx, p, nil)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			} else if err := self.replace(ctx, p, t); err != nil {
				return err
			}
		}
	}
	for parent := Parent(p); parent != ""; parent = Parent(parent) {
		_, err := self.store.Get(ctx, self.name, parent)
		indexed := err == nil
		if err := self.index(ctx, parent); errors.Is(err, fs.ErrNotExist) {
			// It isn't there any more, such as an empty folder which the
			// backend removes, so its parent is indexed again
			continue
		} else if err != nil {
			return err
		}
		if indexed {
			break
		}
	}
	return nil
}

// children returns every object of a folder of the index
func (self *Indexer) children(ctx context.Context, folder string) ([]*schema.Object, error) {
	return self.all(ctx, ListRequest{Parent: folder, Limit: listPage})
}

// beneath returns every object beneath a folder of the index
func (self *Indexer) beneath(ctx context.Context, folder string) ([]*schema.Object, error) {
	return self.all(ctx, ListRequest{Parent: folder, Recursive: true, Limit: listPage})
}

// all returns every object of a listing of the index, a page at a time
func (self *Indexer) all(ctx context.Context, req ListRequest) ([]*schema.Object, error) {
	var result []*schema.Object
	for {
		page, err := self.store.List(ctx, self.name, req)
		if err != nil {
			return nil, err
		}
		result = append(result, page...)
		if len(page) < req.Limit {
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
		if m.Namespace == schema.FolderNamespace && m.Key == "children-count" {
			return object
		}
	}
	object.Meta = append(object.Meta, schema.FolderMeta(count)...)
	return object
}

// mergeCount returns a folder's metadata, from a listing, with the count of
// its objects which the index has, until it's indexed again
func mergeCount(meta, indexed []schema.Meta) []schema.Meta {
	for _, m := range meta {
		if m.Namespace == schema.FolderNamespace && m.Key == "children-count" {
			return meta
		}
	}
	for _, m := range indexed {
		if m.Namespace == schema.FolderNamespace && (m.Key == "children" || m.Key == "children-count") {
			meta = append(meta, m)
		}
	}
	return meta
}
