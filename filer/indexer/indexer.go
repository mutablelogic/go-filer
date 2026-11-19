// Package indexer is a filer which wraps another, and keeps an index of its
// folders, in a Store, which it follows the backend's changes in, so its
// folder tree is listed from the index, rather than the backend, which can
// be slow, such as a bucket, or a network mount. With ModeObjects, its files
// are indexed too, with their attributes, and the backend's metadata, but
// are read, and listed, from the backend, so far. The changes are queued in
// the store, and indexed in the background.
package indexer

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	store "github.com/mutablelogic/go-filer/filer/store"
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
	store store.Store
	queue *queue

	// mu serialises changes to the index, of its worker's work, so a folder
	// isn't reconciled with two listings at once
	mu sync.Mutex

	// live is true while the backend's watch sends its changes as they
	// happen, so the index is up to date, but for the changes which are
	// queued
	live atomic.Bool

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

	// workBatch is how much work is claimed at a time, workLease is how long
	// it's leased for, so it's claimed again, when it isn't done, and
	// workIdle is how often the queue is checked, when there's no work,
	// for work which is due later
	workBatch = 100
	workLease = time.Minute
	workIdle  = time.Second
)

// kinds are the kinds of work which the indexer does
var kinds = []store.Kind{store.KindIndex, store.KindRescan}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns an indexer of the volume's backend, whose index is in the
// store, which Run builds, and maintains
func New(name string, backend gofiler.Filer, s store.Store, opts ...Opt) (*Indexer, error) {
	switch {
	case !types.IsIdentifier(name):
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	case backend == nil:
		return nil, gofiler.ErrBadParameter.With("an indexer needs a backend")
	case s == nil:
		return nil, gofiler.ErrBadParameter.With("an indexer needs a store")
	}
	self := &Indexer{Filer: backend, name: name, store: s}
	if err := self.opt.apply(opts); err != nil {
		return nil, err
	}
	self.queue = newQueue(s, name, self.debounce, self.rescanMin)
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// State returns the state of the index
func (self *Indexer) State(ctx context.Context) (schema.IndexState, error) {
	return self.store.State(ctx, self.name)
}

// Mode returns what's indexed, such as ModeFolders
func (self *Indexer) Mode() Mode {
	return self.mode
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
	query.Set(IndexParam, string(self.mode))
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
	return &Indexer{Filer: backend, opt: self.opt, name: self.name, store: self.store, queue: newQueue(self.store, self.name, self.debounce, self.rescanMin)}, nil
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

// GetObject returns an object: a folder, of the index, once it's ready, when
// the path is a folder's, which ends with "/", or is the root, and a file,
// of the index, when its files are indexed, of a backend whose listing is
// complete (CompleteListing), and it's up to date, and
// otherwise the backend's, which resolves a path without one, which can be
// a file, and a folder, of a bucket. A request which is conditional is the
// backend's.
func (self *Indexer) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	conditional := len(req.IfNoneMatch) > 0 || !req.IfModifiedSince.IsZero()
	isFolder := strings.HasSuffix(req.Path, "/") || folderPath(req.Path) == "/"
	switch {
	case conditional:
		return self.Filer.GetObject(ctx, req)
	case isFolder:
		if self.ready(ctx) {
			if object, err := self.store.Get(ctx, self.name, folderPath(req.Path)); err == nil && object.IsDir {
				object.Name = self.name
				return object, nil
			}
		}
		return self.Filer.GetObject(ctx, req)
	case self.mode != ModeObjects || !completeListing(self.Filer):
		// A file is the backend's, unless files are indexed, and its
		// listing, which the index is of, is as complete as the backend's,
		// which a bucket's isn't, as it hasn't the metadata
		return self.Filer.GetObject(ctx, req)
	}
	return self.getFile(ctx, req)
}

// getFile returns a file, of the index, when it's there, and up to date,
// which it is when the backend is watched live, and it has no work queued,
// or, when it isn't watched live, the backend's is the same. Otherwise it's
// the backend's, and a file which isn't the index's is queued to be indexed.
func (self *Indexer) getFile(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	indexed, err := self.store.Get(ctx, self.name, req.Path)
	if err != nil || indexed.IsDir {
		return self.Filer.GetObject(ctx, req)
	}
	pending, err := self.store.Pending(ctx, self.name, req.Path)
	if err != nil || pending {
		return self.Filer.GetObject(ctx, req)
	}
	indexed.Name = self.name
	if self.live.Load() {
		return indexed, nil
	}

	// It isn't watched live, so the backend's is compared with it
	object, err := self.Filer.GetObject(ctx, req)
	if err != nil {
		return nil, err
	} else if sameObject(indexed, object) {
		return indexed, nil
	}
	if err := self.queue.index(ctx, req.Path); err != nil {
		self.failed(ctx, err)
	}
	return object, nil
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
	if object, err := self.store.Get(ctx, self.name, parent); errors.Is(err, gofiler.ErrNotFound) || (err == nil && !object.IsDir) {
		return nil, &fs.PathError{Op: "list", Path: req.Path, Err: fs.ErrNotExist}
	} else if err != nil {
		return nil, err
	}
	after := ""
	if req.StartAfter != "" {
		after = req.StartAfter
	}
	return &folderIterator{indexer: self, req: store.ListRequest{Parent: parent, Recursive: req.Recursive, After: after, Limit: listPage}}, nil
}

// ListObjects lists objects of the backend, or, when the request is only
// for folders, of their type, inode/directory, or inode, the folders of the
// index, once it's ready
func (self *Indexer) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	folder := &schema.Object{ObjectAttr: schema.ObjectAttr{IsDir: true}}
	file := &schema.Object{ObjectMeta: schema.ObjectMeta{ContentType: "application/octet-stream"}}
	if req.Type != "" && schema.MatchType(folder, req.Type) && !schema.MatchType(file, req.Type) && self.ready(ctx) {
		return self.ListFolders(ctx, req)
	}
	return self.Filer.ListObjects(ctx, req)
}

// CreateObject writes an object to the backend, and queues the index of
// it, which updates its folder, and a folder which is created for it
func (self *Indexer) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	object, err := self.Filer.CreateObject(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := self.queue.index(ctx, object.Path); err != nil {
		self.failed(ctx, err)
	}
	return object, nil
}

// DeleteObjects deletes objects of the backend, and queues the index of
// what's deleted, which updates a folder which is deleted, and its parent
func (self *Indexer) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	resp, err := self.Filer.DeleteObjects(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := self.queue.index(ctx, folderPath(req.Path)); err != nil {
		self.failed(ctx, err)
	}
	return resp, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// ready returns true when the index is ready, so folders are read from it
func (self *Indexer) ready(ctx context.Context) bool {
	state, err := self.store.State(ctx, self.name)
	return err == nil && state.Status == schema.IndexStatusReady
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS - SCANNING

// tree is a folder's subtree, of the backend: the folder, and the folders
// beneath it, by their paths, with how many objects each lists, and its
// files, when they're indexed
type tree struct {
	root    *schema.Object
	folders map[string]*schema.Object
	files   map[string]*schema.Object
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
		// scanned again, and keeps the counts of its last scan until it's
		// scanned, so they aren't of a scan in progress, and otherwise its
		// progress is the folders it's scanned
		if state.Status != schema.IndexStatusReady {
			state.Status, state.Folders, state.Files = schema.IndexStatusScanning, 0, 0
			progress = func(folders int) {
				state.Folders = uint64(folders)
				_ = self.store.SetState(ctx, self.name, state)
			}
		}
		state.Started, state.Error = time.Now(), ""
		if err := self.store.SetState(ctx, self.name, state); err != nil {
			return err
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
		state.Status, state.Folders, state.Files, state.Scanned = schema.IndexStatusReady, uint64(len(t.folders)+1), uint64(t.indexedFiles()), time.Now()
		return self.store.SetState(ctx, self.name, state)
	}
	return nil
}

// list returns a folder's subtree, of one recursive listing of the backend,
// reporting how many folders it's listed every so often, or fs.ErrNotExist,
// with an empty subtree, when the folder isn't there
func (self *Indexer) list(ctx context.Context, root string, progress func(int)) (tree, error) {
	t := tree{folders: make(map[string]*schema.Object), files: make(map[string]*schema.Object), counts: map[string]int{root: 0}}
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
		t.counts[store.Parent(child.Path)]++
		if child.IsDir {
			t.folders[child.Path] = child
			if _, exists := t.counts[child.Path]; !exists {
				t.counts[child.Path] = 0
			}
			if progress != nil && len(t.folders)%progressEvery == 0 {
				progress(len(t.folders))
			}
		} else if self.mode == ModeObjects {
			t.files[child.Path] = child
		}
	}
}

// replace replaces the index's subtree of a folder with the backend's,
// removing the objects which aren't there any more, or aren't indexed, with
// what's beneath them, or the whole subtree, when the folder isn't there.
// A folder and a file of the same path, as a bucket can have, is the
// folder. It's written a page at a time. It's called with the indexer
// locked.
func (self *Indexer) replace(ctx context.Context, root string, t tree) error {
	if t.root == nil {
		return self.store.Delete(ctx, self.name, root)
	}
	indexed, err := self.beneath(ctx, root)
	if err != nil {
		return err
	}
	files := make(map[string]*schema.Object)
	for _, object := range indexed {
		if !t.has(object) {
			if err := self.store.Delete(ctx, self.name, object.Path); err != nil {
				return err
			}
		} else if !object.IsDir {
			files[object.Path] = object
		}
	}
	objects := make([]*schema.Object, 0, len(t.folders)+len(t.files)+1)
	t.root.Path = root
	objects = append(objects, withCount(t.root, t.counts[root]))
	for p, folder := range t.folders {
		objects = append(objects, withCount(folder, t.counts[p]))
	}
	return self.put(ctx, append(objects, t.changed(files)...))
}

// changed returns the files of the tree which are indexed, and aren't the
// same as those which are, by their entity tags, or sizes and modified
// times, so a file which is stored of the backend, which can have more than
// its listing, such as a bucket's metadata, isn't replaced by its listing
func (t tree) changed(indexed map[string]*schema.Object) []*schema.Object {
	result := make([]*schema.Object, 0, len(t.files))
	for p, file := range t.files {
		if _, exists := t.folders[p]; exists {
			continue
		} else if previous, exists := indexed[p]; exists && sameObject(previous, file) {
			continue
		}
		result = append(result, file)
	}
	return result
}

// indexedFiles returns how many files of the tree are indexed, which are
// those which aren't of a folder's path
func (t tree) indexedFiles() int {
	n := 0
	for p := range t.files {
		if _, exists := t.folders[p]; !exists {
			n++
		}
	}
	return n
}

// has returns true when the tree has the object, which is indexed: a
// folder, or a file, when the files are, as a file isn't of the folders
func (t tree) has(object *schema.Object) bool {
	if object.IsDir {
		_, exists := t.folders[object.Path]
		return exists
	}
	_, exists := t.files[object.Path]
	return exists
}

// put stores the objects, a page at a time, without their volume's name
func (self *Indexer) put(ctx context.Context, objects []*schema.Object) error {
	for _, object := range objects {
		object.Name = ""
	}
	for len(objects) > 0 {
		n := min(len(objects), listPage)
		if err := self.store.Put(ctx, self.name, objects[:n]...); err != nil {
			return err
		}
		objects = objects[n:]
	}
	return nil
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
	t := tree{folders: make(map[string]*schema.Object), files: make(map[string]*schema.Object)}
	for {
		child, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		count++
		if child.IsDir {
			t.folders[child.Path] = child
		} else if self.mode == ModeObjects {
			t.files[child.Path] = child
		}
	}

	// Its objects which aren't there any more, or aren't indexed, are
	// removed, and its folders which are keep the counts the index has of
	// them, until they're indexed
	indexed, err := self.children(ctx, folder)
	if err != nil {
		return err
	}
	counts := make(map[string][]schema.Meta)
	files := make(map[string]*schema.Object)
	for _, child := range indexed {
		if !t.has(child) {
			if err := self.store.Delete(ctx, self.name, child.Path); err != nil {
				return err
			}
		} else if child.IsDir {
			counts[child.Path] = child.Meta
		} else {
			files[child.Path] = child
		}
	}
	object.Path = folder
	objects := []*schema.Object{withCount(object, count)}
	for p, child := range t.folders {
		child.Meta = mergeCount(child.Meta, counts[p])
		objects = append(objects, child)
	}
	return self.put(ctx, append(objects, t.changed(files)...))
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
	var full *schema.Object
	if p != "/" {
		object, err := self.Filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: dirPath(p)}})
		indexed, indexErr := self.store.Get(ctx, self.name, p)
		if indexErr != nil && !errors.Is(indexErr, gofiler.ErrNotFound) {
			return indexErr
		}
		wasFile := indexErr == nil && !indexed.IsDir
		wasFolder := indexErr == nil && indexed.IsDir
		switch {
		case errors.Is(err, fs.ErrNotExist) || (err == nil && !object.IsDir):
			// It isn't a folder, or isn't there any more. A file which was
			// indexed, and still is, is stored, of the backend, and its
			// folder's count is the same, so it isn't indexed again. A file
			// which is new is stored by its folder's index, and then of the
			// backend, so a folder which fails to be indexed is retried.
			if self.mode == ModeObjects {
				file, err := self.file(ctx, p)
				if err != nil {
					return err
				} else if file != nil && wasFile {
					return self.put(ctx, []*schema.Object{file})
				} else if file != nil {
					full = file
				}
			}
			if full == nil || wasFolder {
				if err := self.store.Delete(ctx, self.name, p); err != nil {
					return err
				}
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

	// The folders above it are indexed again, up to the first which was
	// indexed, as a folder, rather than a file of the same path
	for parent := store.Parent(p); parent != ""; parent = store.Parent(parent) {
		object, err := self.store.Get(ctx, self.name, parent)
		if err != nil && !errors.Is(err, gofiler.ErrNotFound) {
			return err
		}
		indexed := err == nil && object.IsDir
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

	// A new file is stored of the backend, which can have more than its
	// listing, such as a bucket's metadata
	if full != nil {
		return self.put(ctx, []*schema.Object{full})
	}
	return nil
}

// file returns the file of the backend at the path, or nil, when there's
// none, or it's a folder
func (self *Indexer) file(ctx context.Context, p string) (*schema.Object, error) {
	object, err := self.Filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: p}})
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, gofiler.ErrNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else if object.IsDir {
		return nil, nil
	}
	object.Path = p
	return object, nil
}

// children returns every object of a folder of the index
func (self *Indexer) children(ctx context.Context, folder string) ([]*schema.Object, error) {
	return self.all(ctx, store.ListRequest{Parent: folder, Limit: listPage})
}

// beneath returns every object beneath a folder of the index
func (self *Indexer) beneath(ctx context.Context, folder string) ([]*schema.Object, error) {
	return self.all(ctx, store.ListRequest{Parent: folder, Recursive: true, Limit: listPage})
}

// all returns every object of a listing of the index, a page at a time
func (self *Indexer) all(ctx context.Context, req store.ListRequest) ([]*schema.Object, error) {
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
		req.After = store.Key(page[len(page)-1])
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// completeListing returns true when a backend's listing has its files as
// GetObject returns them, so the index, of its listings, is
func completeListing(backend gofiler.Filer) bool {
	c, ok := backend.(interface{ CompleteListing() bool })
	return ok && c.CompleteListing()
}

// sameObject returns true when two versions of a file are the same, of their
// entity tags, when they both have one, and otherwise of their sizes, and
// when they were modified
func sameObject(a, b *schema.Object) bool {
	if a.ETag != nil && b.ETag != nil {
		return *a.ETag == *b.ETag
	}
	sameSize := (a.Size == nil) == (b.Size == nil) && (a.Size == nil || *a.Size == *b.Size)
	return sameSize && a.ModTime.Equal(b.ModTime)
}

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
