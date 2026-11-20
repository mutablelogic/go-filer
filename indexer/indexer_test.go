package indexer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fs "github.com/mutablelogic/go-filer/backend/fs"
	fsevents "github.com/mutablelogic/go-filer/backend/fs/fsevents"
	indexer "github.com/mutablelogic/go-filer/indexer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////
// HELPERS

// newBackend returns a filesystem backend of a new directory, with folders
// and files, which it returns
func newBackend(t *testing.T) (gofiler.Filer, string) {
	t.Helper()
	dir := t.TempDir()
	for _, p := range []string{"a/b", "c"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, filepath.FromSlash(p)), 0o755))
	}
	for _, p := range []string{"x.txt", "a/y.txt"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, filepath.FromSlash(p)), []byte(p), 0o644))
	}
	backend, err := fs.New("media", dir, nil, gofiler.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	return backend, dir
}

// run runs the indexer, until the test ends, and waits for its index to be
// ready
func run(t *testing.T, i *indexer.Indexer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- i.Run(ctx, nil) }()
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, <-done)
	})
	require.Eventually(t, func() bool {
		state, err := i.State(context.Background())
		return err == nil && state.Status == schema.IndexStatusReady
	}, 10*time.Second, 5*time.Millisecond, "the index isn't ready")
}

// folders lists the folders of a folder, or beneath it
func folders(t *testing.T, i *indexer.Indexer, path string, recursive bool) []string {
	t.Helper()
	iter, err := i.ListFolders(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: path}, Recursive: recursive})
	require.NoError(t, err)
	defer iter.Close()
	result := []string{}
	for {
		object, err := iter.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return result
		}
		require.NoError(t, err)
		assert.True(t, object.IsDir)
		result = append(result, object.Path)
	}
}

// count returns a folder's folder:children-count
func count(t *testing.T, object *schema.Object) string {
	t.Helper()
	for _, m := range object.Meta {
		if m.Namespace == schema.FolderNamespace && m.Key == "children-count" {
			return string(m.Value)
		}
	}
	return ""
}

// eventually waits for the folders beneath the root to be the ones wanted
func eventually(t *testing.T, i *indexer.Indexer, want ...string) {
	t.Helper()
	require.Eventually(t, func() bool {
		got := folders(t, i, "/", true)
		return strings.Join(got, ",") == strings.Join(want, ",")
	}, 10*time.Second, 10*time.Millisecond, "want %v, got %v", want, folders(t, i, "/", true))
}

// unwatched is a backend which can't be watched, or whose watch fails, the
// first time
type unwatched struct {
	gofiler.Filer
	fail    bool
	watches atomic.Int32
}

func (u *unwatched) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	if u.watches.Add(1) == 1 && u.fail {
		return gofiler.ErrServiceUnavailable.With("the watch failed")
	} else if !u.fail {
		return gofiler.ErrNotImplemented.With("it can't be watched")
	}
	return u.Filer.WatchObjects(ctx, ch)
}

// counting is a backend which counts its calls to GetObject, whose watch is
// live when the backend's is
type counting struct {
	gofiler.Filer
	gets atomic.Int32
}

func (c *counting) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	c.gets.Add(1)
	return c.Filer.GetObject(ctx, req)
}

// unwatchedCounting is a backend which can't be watched, which counts its
// calls to GetObject
type unwatchedCounting struct {
	counting
}

func (u *unwatchedCounting) WatchObjects(context.Context, chan<- schema.Event) error {
	return gofiler.ErrNotImplemented.With("it can't be watched")
}

// gets returns how many times a file is got from the backend, of a call to
// GetObject of the indexer, and the file
func gets(t *testing.T, i *indexer.Indexer, c *counting, req schema.GetObjectRequest) (int32, *schema.Object) {
	t.Helper()
	before := c.gets.Load()
	object, err := i.GetObject(context.Background(), req)
	require.NoError(t, err)
	return c.gets.Load() - before, object
}

// bursty is a backend whose watch sends a burst of rescans of the root, and
// which counts the recursive listings of it, which are scans
type bursty struct {
	gofiler.Filer
	burst int
	more  chan struct{} // Sends another burst
	scans atomic.Int32
}

func (b *bursty) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	for {
		for range b.burst {
			select {
			case ch <- schema.Rescan(""):
			case <-ctx.Done():
				return nil
			}
		}
		select {
		case <-b.more:
		case <-ctx.Done():
			return nil
		}
	}
}

func (b *bursty) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	if req.Recursive {
		b.scans.Add(1)
	}
	return b.Filer.ListObjects(ctx, req)
}

////////////////////////////////////////////////////////////////////////////////
// TESTS

func Test_New(t *testing.T) {
	backend, _ := newBackend(t)
	for name, test := range map[string]struct {
		name    string
		backend gofiler.Filer
		store   store.Store
	}{
		"Name":    {"not a name", backend, newStore(t)},
		"Backend": {"media", nil, newStore(t)},
		"Store":   {"media", backend, nil},
	} {
		_, err := indexer.New(test.name, test.backend, test.store)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, name)
	}
	_, err := indexer.New("media", backend, newStore(t), indexer.WithRescanInterval(0))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}

func Test_Scan(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, _ := newBackend(t)
	s := newStore(t)
	i, err := indexer.New("media", backend, s)
	require.NoError(t, err)

	// Until it's indexed, its folders are the backend's
	state, err := i.State(context.Background())
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, state.Status)
	assert.Equal(t, []string{"/a", "/a/b", "/c"}, folders(t, i, "/", true))

	// Once it's indexed, they're the index's: its folders, in the order a
	// backend lists them, of a folder, or beneath it, with the count of
	// each folder's objects
	run(t, i)
	state, err = i.State(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(4), state.Folders, "the root, and its three folders")
	assert.False(t, state.Scanned.IsZero())
	assert.Equal(t, []string{"/a", "/c"}, folders(t, i, "/", false))
	assert.Equal(t, []string{"/a", "/a/b", "/c"}, folders(t, i, "/", true))
	assert.Equal(t, []string{"/a/b"}, folders(t, i, "/a/", false))
	object, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a/"}})
	require.NoError(t, err)
	assert.True(t, object.IsDir)
	assert.Equal(t, "media", object.Name)
	assert.Equal(t, "2", count(t, object), "its folder, and its file")
	indexed, err := s.GetObject(context.Background(), "media", "/a/")
	require.NoError(t, err)
	assert.Equal(t, "/a", indexed.Path, "it's got from the index")

	// A file isn't indexed, so it's the backend's, and a folder which isn't
	// there isn't found
	object, err = i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/x.txt"}})
	require.NoError(t, err)
	assert.False(t, object.IsDir)
	_, err = i.ListFolders(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/missing/"}})
	assert.Error(t, err)
}

func Test_Watch(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	} else if runtime.GOOS == "windows" {
		t.Skip("a directory which is watched can't be removed on Windows")
	}
	backend, dir := newBackend(t)
	i, err := indexer.New("media", backend, newStore(t))
	require.NoError(t, err)
	run(t, i)

	// Folders which are created, and removed, on the backend, follow it
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "d", "e"), 0o755))
	eventually(t, i, "/a", "/a/b", "/c", "/d", "/d/e")
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "a")))
	eventually(t, i, "/c", "/d", "/d/e")
}

func Test_Write(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, _ := newBackend(t)
	i, err := indexer.New("media", backend, newStore(t))
	require.NoError(t, err)
	run(t, i)

	// A file which is written through it, in folders which are created for
	// it, is indexed, in the background, with the count of the folder's
	// objects
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/f/g/z.txt"}, Body: strings.NewReader("z")})
	require.NoError(t, err)
	eventually(t, i, "/a", "/a/b", "/c", "/f", "/f/g")
	object, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/f/g/"}})
	require.NoError(t, err)
	assert.Equal(t, "1", count(t, object))

	// A folder which is deleted through it, with what's beneath it, is
	// removed from the index
	_, err = i.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a/"}, Recursive: true})
	require.NoError(t, err)
	eventually(t, i, "/c", "/f", "/f/g")
}

func Test_RescanBurst(t *testing.T) {
	backend, _ := newBackend(t)

	// A burst of rescans, from the backend, is coalesced, into one scan, and
	// once it's scanned, another burst is deferred, by the minimum interval,
	// unless there's none
	for _, test := range []struct {
		interval time.Duration
		scans    int32
	}{{0, 2}, {time.Minute, 1}} {
		b := &bursty{Filer: backend, burst: 20, more: make(chan struct{})}
		i, err := indexer.New("media", b, newStore(t), indexer.WithDebounce(500*time.Millisecond), indexer.WithRescanMinInterval(test.interval))
		require.NoError(t, err)
		run(t, i)
		assert.Equal(t, []string{"/a", "/a/b", "/c"}, folders(t, i, "/", true))
		time.Sleep(300 * time.Millisecond)
		require.Equal(t, int32(1), b.scans.Load(), "one scan of the burst")
		b.more <- struct{}{}
		time.Sleep(2 * time.Second)
		assert.Equal(t, test.scans, b.scans.Load(), "with a minimum interval of %v", test.interval)
	}
}

func Test_Clone(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, _ := newBackend(t)
	i, err := indexer.New("media", backend, newStore(t))
	require.NoError(t, err)

	// A clone is an indexer of the backend's clone, with its own queue, which
	// runs, and whose writes are indexed
	clone, err := i.Clone(context.Background(), nil)
	require.NoError(t, err)
	c, ok := clone.(*indexer.Indexer)
	require.True(t, ok)
	t.Cleanup(func() { c.Close() })
	run(t, c)
	_, err = c.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/g/z.txt"}, Body: strings.NewReader("z")})
	require.NoError(t, err)
	eventually(t, c, "/a", "/a/b", "/c", "/g")
}

// indexed returns the paths of the objects of the store's index, of the
// volume media, which are files, in their order
func indexed(t *testing.T, s store.Store) []string {
	t.Helper()
	objects, err := s.ListObjects(context.Background(), "media", store.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	result := []string{}
	for _, object := range objects {
		if !object.IsDir {
			result = append(result, object.Path)
		}
	}
	return result
}

// files waits for the files of the store's index to be those
func files(t *testing.T, s store.Store, want ...string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return strings.Join(indexed(t, s), ",") == strings.Join(want, ",")
	}, 10*time.Second, 10*time.Millisecond, "want %v, got %v", want, indexed(t, s))
}

func Test_Objects(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, dir := newBackend(t)
	s := newStore(t)

	// Its files are indexed, with their attributes, as well as its folders,
	// which its URL says
	i, err := indexer.New("media", backend, s, indexer.WithMode(indexer.ModeObjects), indexer.WithDebounce(0))
	require.NoError(t, err)
	assert.Equal(t, indexer.ModeObjects, i.Mode())
	assert.Equal(t, "objects", i.URL().Query().Get(indexer.IndexParam))
	run(t, i)
	assert.Equal(t, []string{"/a/y.txt", "/x.txt"}, indexed(t, s))
	assert.Equal(t, []string{"/a", "/a/b", "/c"}, folders(t, i, "/", true), "its folders are listed, not its files")
	state, err := i.State(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(4), state.Folders, "with the root")
	assert.Equal(t, uint64(2), state.Files)
	file, err := s.GetObject(context.Background(), "media", "/x.txt")
	require.NoError(t, err)
	require.NotNil(t, file.Size)
	assert.Equal(t, int64(len("x.txt")), *file.Size)
	assert.False(t, file.ModTime.IsZero())

	// A file which is written through it, or the backend, is indexed, and
	// one which changes is indexed again
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c/z.txt"}, Body: strings.NewReader("z")})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "w.txt"), []byte("w"), 0o644))
	files(t, s, "/a/y.txt", "/c/z.txt", "/w.txt", "/x.txt")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "x.txt"), []byte("longer"), 0o644))
	require.Eventually(t, func() bool {
		file, err := s.GetObject(context.Background(), "media", "/x.txt")
		return err == nil && file.Size != nil && *file.Size == int64(len("longer"))
	}, 10*time.Second, 10*time.Millisecond, "its size is indexed again")
	require.Eventually(t, func() bool {
		object, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c/"}})
		return err == nil && count(t, object) == "1"
	}, 10*time.Second, 10*time.Millisecond, "its folder's count")

	// A file which is deleted, through it, or the backend, isn't, nor are
	// the files of a folder which is removed
	_, err = i.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/c/z.txt"}})
	require.NoError(t, err)
	require.NoError(t, os.Remove(filepath.Join(dir, "w.txt")))
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "a")))
	files(t, s, "/x.txt")
}

func Test_ObjectsToFolders(t *testing.T) {
	backend, _ := newBackend(t)
	s := newStore(t)

	// A volume whose files were indexed, which is indexed of its folders,
	// doesn't keep its files, once it's scanned
	i, err := indexer.New("media", &unwatched{Filer: backend}, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- i.Run(ctx, nil) }()
	files(t, s, "/a/y.txt", "/x.txt")
	require.Eventually(t, func() bool {
		state, err := s.State(context.Background(), "media")
		return err == nil && state.Status == schema.IndexStatusReady && state.Files == 2
	}, 10*time.Second, 5*time.Millisecond, "its scan is done")
	cancel()
	require.NoError(t, <-done)
	before, err := s.State(context.Background(), "media")
	require.NoError(t, err)
	require.Equal(t, uint64(4), before.Folders)
	assert.Equal(t, string(indexer.ModeObjects), before.Mode, "its scan indexed its files")

	// While it's scanned again, its counts are of its last scan, rather than
	// of the scan in progress
	lowest := before.Folders
	watching, stop := context.WithCancel(context.Background())
	watched := make(chan struct{})
	go func() {
		defer close(watched)
		for watching.Err() == nil {
			if state, err := s.State(context.Background(), "media"); err == nil && state.Folders < lowest {
				lowest = state.Folders
			}
			time.Sleep(time.Millisecond)
		}
	}()
	folders, err := indexer.New("media", &unwatched{Filer: backend}, s, indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, folders)
	files(t, s)
	stop()
	<-watched
	assert.Equal(t, before.Folders, lowest, "its count isn't the progress of the scan")

	// Its count of files, and what it indexed, are stored once the scan is
	// done, after its files are removed
	assert.Eventually(t, func() bool {
		after, err := s.State(context.Background(), "media")
		return err == nil && after.Files == 0 && after.Mode == string(indexer.ModeFolders)
	}, 10*time.Second, 5*time.Millisecond, "it has no files, of a scan of its folders")
}

// faultyStore is a store whose reads of a path fail, and whose listings
// fail, while it's set to
type faultyStore struct {
	store.Store
	path  string       // A path whose reads fail
	lists atomic.Int32 // How many listings fail
}

func (f *faultyStore) GetObject(ctx context.Context, volume, path string) (*schema.Object, error) {
	if f.path != "" && path == f.path {
		return nil, gofiler.ErrInternalServerError.With("the read failed")
	}
	return f.Store.GetObject(ctx, volume, path)
}

func (f *faultyStore) ListObjects(ctx context.Context, volume string, req store.ListRequest) ([]*schema.Object, error) {
	if f.lists.Add(-1) >= 0 {
		return nil, gofiler.ErrInternalServerError.With("the listing failed")
	}
	return f.Store.ListObjects(ctx, volume, req)
}

// fuller is a backend whose files have more metadata than its listing, as a
// bucket's do
type fuller struct {
	gofiler.Filer
}

func (f *fuller) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	object, err := f.Filer.GetObject(ctx, req)
	if err == nil && !object.IsDir {
		object.Meta = append(object.Meta, schema.Meta{Namespace: "dc", Key: "title", Value: json.RawMessage(`"full"`)})
	}
	return object, err
}

// revised is a backend which can't be watched, whose files' entity tags are
// the same, as a bucket's are when only their metadata is written, whose
// listing's modification time is its version, as a bucket's is when it was
// written, and whose metadata, which its listing hasn't, is its version, with
// a modification time which is the same, as a bucket's of its metadata is
type revised struct {
	gofiler.Filer
	version atomic.Int32
	gets    atomic.Int32 // How many files it's got
}

func (r *revised) WatchObjects(context.Context, chan<- schema.Event) error {
	return gofiler.ErrNotImplemented.With("it can't be watched")
}

func (r *revised) stamp(object *schema.Object, full bool) {
	if object.IsDir {
		return
	}
	etag, version := "same", r.version.Load()
	object.ETag = &etag
	if full {
		object.ModTime = time.Unix(1, 0)
		object.Meta = []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(fmt.Sprintf(`"%d"`, version))}}
	} else {
		object.ModTime = time.Unix(int64(version), 0)
	}
}

func (r *revised) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	object, err := r.Filer.GetObject(ctx, req)
	if err == nil {
		if !object.IsDir {
			r.gets.Add(1)
		}
		r.stamp(object, true)
	}
	return object, err
}

func (r *revised) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	iter, err := r.Filer.ListObjects(ctx, req)
	if err != nil {
		return nil, err
	}
	return &revisedIterator{ListObjectsIterator: iter, r: r}, nil
}

type revisedIterator struct {
	schema.ListObjectsIterator
	r *revised
}

func (i *revisedIterator) Next(ctx context.Context) (*schema.Object, error) {
	object, err := i.ListObjectsIterator.Next(ctx)
	if err == nil {
		i.r.stamp(object, false)
	}
	return object, err
}

func Test_ObjectsRevision(t *testing.T) {
	backend, _ := newBackend(t)
	r := &revised{Filer: backend}
	r.version.Store(1)
	s := newStore(t)

	// A file whose entity tag is the same, but whose listing isn't, such as a
	// bucket's whose metadata is written, is stored again, of the backend,
	// when it's scanned again, and one whose listing is the same isn't
	i, err := indexer.New("media", r, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(20*time.Millisecond), indexer.WithRescanMinInterval(0), indexer.WithDebounce(0))
	require.NoError(t, err)
	run(t, i)
	title := func(want string) func() bool {
		return func() bool {
			object, err := s.GetObject(context.Background(), "media", "/x.txt")
			return err == nil && len(object.Meta) == 1 && string(object.Meta[0].Value) == `"`+want+`"`
		}
	}
	require.Eventually(t, title("1"), 10*time.Second, 10*time.Millisecond, "it's stored of the backend")
	gets := r.gets.Load()
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, gets, r.gets.Load(), "it isn't got again, while its listing is the same")
	r.version.Store(2)
	require.Eventually(t, title("2"), 10*time.Second, 10*time.Millisecond, "it's stored again, of its revision")
}

func Test_ObjectsReadError(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, _ := newBackend(t)

	// A file whose index can't be read is an error, which is retried, of
	// the index's state, rather than a panic
	s := &faultyStore{Store: newStore(t)}
	i, err := indexer.New("media", backend, s, indexer.WithMode(indexer.ModeObjects), indexer.WithDebounce(0))
	require.NoError(t, err)
	run(t, i)
	s.path = "/c/z.txt"
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c/z.txt"}, Body: strings.NewReader("z")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		state, err := i.State(context.Background())
		return err == nil && strings.Contains(state.Error, "the read failed")
	}, 10*time.Second, 10*time.Millisecond)
}

func Test_ListFoldersOfFile(t *testing.T) {
	backend, _ := newBackend(t)

	// A file of the index isn't a folder, so it can't be listed
	i, err := indexer.New("media", &unwatched{Filer: backend}, newStore(t), indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, i)
	_, err = i.ListFolders(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/x.txt/"}})
	assert.ErrorIs(t, err, iofs.ErrNotExist)
}

func Test_ObjectsKeepMeta(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, _ := newBackend(t)
	s := newStore(t)

	// A file is stored of the backend, with more than its listing, which
	// isn't replaced by its listing, while it's the same, when its folder is
	// indexed again
	i, err := indexer.New("media", &fuller{Filer: backend}, s, indexer.WithMode(indexer.ModeObjects), indexer.WithDebounce(0))
	require.NoError(t, err)
	run(t, i)
	title := func(path string) bool {
		object, err := s.GetObject(context.Background(), "media", path)
		return err == nil && len(object.Meta) == 1 && string(object.Meta[0].Value) == `"full"`
	}

	// The files of its scan are stored of the backend, rather than its
	// listing, which has less
	assert.True(t, title("/x.txt"), "a file of its scan is stored of the backend")
	assert.True(t, title("/a/y.txt"), "a file of its scan is stored of the backend")
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c/z.txt"}, Body: strings.NewReader("z")})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return title("/c/z.txt") }, 10*time.Second, 10*time.Millisecond, "it's stored of the backend")
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c/w.txt"}, Body: strings.NewReader("w")})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return title("/c/w.txt") }, 10*time.Second, 10*time.Millisecond)
	assert.True(t, title("/c/z.txt"), "it's kept, as it's the same")
}

func Test_BucketFileAndFolder(t *testing.T) {
	// A bucket's file, /a, beside which a folder, /a/, is created, isn't
	// mistaken for the folder, so the root, which lists both, is indexed
	// again, and both are indexed, of their keys
	b := newBucket("a", "x.txt")
	s := newStore(t)
	i, err := indexer.New("media", b, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour), indexer.WithDebounce(0))
	require.NoError(t, err)
	run(t, i)
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a/b/c.txt"}, Body: strings.NewReader("c")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		root, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
		return err == nil && count(t, root) == "3"
	}, 10*time.Second, 10*time.Millisecond, "a, a/ and x.txt")
	require.Eventually(t, func() bool {
		file, fileErr := s.GetObject(context.Background(), "media", "/a")
		folder, folderErr := s.GetObject(context.Background(), "media", "/a/")
		return fileErr == nil && !file.IsDir && folderErr == nil && folder.IsDir
	}, 10*time.Second, 10*time.Millisecond, "the file, and the folder, are indexed")
	file, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a"}})
	require.NoError(t, err)
	assert.False(t, file.IsDir, "the file is got of its key")
	folder, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a/"}})
	require.NoError(t, err)
	assert.True(t, folder.IsDir, "and the folder of its")

	// A listing of the index has both, as the bucket's does
	req := schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}
	want, err := listed(t, b, req)
	require.NoError(t, err)
	got, err := listed(t, i, req)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Contains(t, got, "/a")
	assert.Contains(t, got, "/a/")
}

func Test_ObjectsParentRetry(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, _ := newBackend(t)

	// A new file whose folder fails to be indexed is indexed again, with its
	// folder, rather than being taken as indexed
	s := &faultyStore{Store: newStore(t)}
	i, err := indexer.New("media", backend, s, indexer.WithMode(indexer.ModeObjects), indexer.WithDebounce(0), indexer.WithRetry(10*time.Millisecond))
	require.NoError(t, err)
	run(t, i)
	time.Sleep(100 * time.Millisecond)
	s.lists.Store(1)
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c/z.txt"}, Body: strings.NewReader("z")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		object, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c/"}})
		return err == nil && count(t, object) == "1"
	}, 10*time.Second, 10*time.Millisecond, "its folder's count")
}

func Test_GetFile(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, _ := newBackend(t)
	c := &counting{Filer: backend}
	s := newStore(t)
	x := schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/x.txt"}}

	// A file of a volume which is watched is the index's, without the
	// backend, with its volume's name
	i, err := indexer.New("media", c, s, indexer.WithMode(indexer.ModeObjects))
	require.NoError(t, err)
	run(t, i)
	files(t, s, "/a/y.txt", "/x.txt")
	var object *schema.Object
	require.Eventually(t, func() bool {
		var n int32
		n, object = gets(t, i, c, x)
		return n == 0
	}, 10*time.Second, 10*time.Millisecond, "it's the index's, once the changes of its creation are indexed")
	assert.Equal(t, "media", object.Name)
	assert.Equal(t, int64(len("x.txt")), *object.Size)

	// But not a file which isn't indexed, a request which is conditional, or
	// a file which has work queued, which would change it
	require.Eventually(t, func() bool {
		n, _ := gets(t, i, c, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a/"}})
		return n == 0
	}, 10*time.Second, 10*time.Millisecond, "a folder is the index's")
	var n int32
	n, _ = gets(t, i, c, schema.GetObjectRequest{ObjectKey: x.ObjectKey, IfModifiedSince: time.Unix(1, 0)})
	assert.Equal(t, int32(1), n, "a conditional request is the backend's")
	require.NoError(t, s.Push(context.Background(), "media", store.Work{Key: "/x.txt", Kind: store.KindIndex, After: time.Now().Add(time.Hour)}))
	n, _ = gets(t, i, c, x)
	assert.Equal(t, int32(1), n, "a file with work queued is the backend's")

	// A volume whose files aren't indexed gets them from the backend
	folders, err := indexer.New("media", c, newStore(t))
	require.NoError(t, err)
	run(t, folders)
	n, _ = gets(t, folders, c, x)
	assert.Equal(t, int32(1), n)
}

func Test_GetFileUnwatched(t *testing.T) {
	backend, dir := newBackend(t)
	c := &unwatchedCounting{counting{Filer: backend}}
	s := newStore(t)
	x := schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/x.txt"}}

	// A file of a volume which is polled, rather than watched, is the
	// index's too, as of its last scan, between its scans, while none is
	// queued
	i, err := indexer.New("media", c, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(50*time.Millisecond), indexer.WithRescanMinInterval(0), indexer.WithDebounce(0))
	require.NoError(t, err)
	run(t, i)
	files(t, s, "/a/y.txt", "/x.txt")
	require.Eventually(t, func() bool {
		n, object := gets(t, i, &c.counting, x)
		return n == 0 && *object.Size == int64(len("x.txt"))
	}, 10*time.Second, time.Millisecond, "it's the index's")

	// One which changed is the index's once it's scanned again
	require.NoError(t, os.WriteFile(filepath.Join(dir, "x.txt"), []byte("longer"), 0o644))
	require.Eventually(t, func() bool {
		_, object := gets(t, i, &c.counting, x)
		return *object.Size == int64(len("longer"))
	}, 10*time.Second, 10*time.Millisecond, "it's scanned again")
}

func Test_RescanClearsError(t *testing.T) {
	backend, _ := newBackend(t)
	s := newStore(t)
	i, err := indexer.New("media", &unwatched{Filer: backend}, s, indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, i)

	// A rescan, of a folder, or of the root, clears why the index last
	// failed, as it starts
	for _, folder := range []string{"/a/", "/"} {
		state, err := s.State(context.Background(), "media")
		require.NoError(t, err)
		state.Error = "it failed"
		require.NoError(t, s.SetState(context.Background(), "media", state))
		require.NoError(t, s.Push(context.Background(), "media", store.Work{Key: folder, Kind: store.KindRescan}))
		require.Eventually(t, func() bool {
			state, err := s.State(context.Background(), "media")
			return err == nil && state.Error == ""
		}, 10*time.Second, 10*time.Millisecond, "a rescan of %s clears it", folder)
	}
}

// listing is a backend which can't be watched, which counts its listings,
// and the files it gets
type listing struct {
	gofiler.Filer
	lists atomic.Int32
	gets  atomic.Int32
}

func (l *listing) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	object, err := l.Filer.GetObject(ctx, req)
	if err == nil && !object.IsDir {
		l.gets.Add(1)
	}
	return object, err
}

func (l *listing) WatchObjects(context.Context, chan<- schema.Event) error {
	return gofiler.ErrNotImplemented.With("it can't be watched")
}

func (l *listing) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	l.lists.Add(1)
	return l.Filer.ListObjects(ctx, req)
}

// listed returns the keys of a listing, or the error, of a type, which the
// registry filters
func listed(t *testing.T, f gofiler.Filer, req schema.ListObjectsRequest) ([]string, error) {
	t.Helper()
	iter, err := f.ListObjects(context.Background(), req)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	result := []string{}
	for {
		object, err := iter.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return result, nil
		} else if err != nil {
			return nil, err
		} else if schema.MatchType(object, req.Type) {
			result = append(result, object.Key())
		}
	}
}

func Test_ListObjects(t *testing.T) {
	backend, dir := newBackend(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a", "b", "z.png"), []byte("z"), 0o644))
	l := &listing{Filer: backend}
	s := newStore(t)
	i, err := indexer.New("media", l, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, i)

	// A listing, of a folder, or beneath it, or of a file, after where it
	// starts, of a type, is the index's, without the backend, and is the
	// same as the backend's
	for _, req := range []schema.ListObjectsRequest{
		{ObjectKey: schema.ObjectKey{Path: "/"}},
		{ObjectKey: schema.ObjectKey{Path: ""}, Recursive: true},
		{ObjectKey: schema.ObjectKey{Path: "/a/"}},
		{ObjectKey: schema.ObjectKey{Path: "a"}},
		{ObjectKey: schema.ObjectKey{Path: "/a"}, Recursive: true},
		{ObjectKey: schema.ObjectKey{Path: "/x.txt"}},
		{ObjectKey: schema.ObjectKey{Path: "/x.txt"}, StartAfter: "/x.txt"},
		{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true, StartAfter: "/a/"},
		{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true, StartAfter: "a/b/"},
		{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true, Type: "image"},
		{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true, Type: "directory"},
	} {
		want, err := listed(t, backend, req)
		require.NoError(t, err, req)
		before := l.lists.Load()
		got, err := listed(t, i, req)
		require.NoError(t, err, req)
		assert.Equal(t, want, got, req)
		assert.Equal(t, before, l.lists.Load(), "%+v is the index's", req)
	}

	// But a folder which has work queued, at it, beneath it, or above it,
	// or a path which isn't indexed, is the backend's
	before := l.lists.Load()
	require.NoError(t, s.Push(context.Background(), "media", store.Work{Key: "/a/b/z.png", Kind: store.KindIndex, After: time.Now().Add(time.Hour)}))
	_, err = listed(t, i, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a/"}})
	require.NoError(t, err)
	assert.Equal(t, before+1, l.lists.Load(), "work beneath it")
	_, err = listed(t, i, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a/"}, Type: "directory"})
	require.NoError(t, err)
	assert.Equal(t, before+2, l.lists.Load(), "of its folders, too")
	_, err = listed(t, i, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/c/"}})
	require.NoError(t, err)
	assert.Equal(t, before+2, l.lists.Load(), "but not of another folder")
	_, err = listed(t, i, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/missing/"}})
	assert.ErrorIs(t, err, iofs.ErrNotExist)
	assert.Equal(t, before+3, l.lists.Load(), "a path which isn't indexed")

	// And a path, without a "/" after it, of a folder, which a file of the
	// same path has work queued of, as it can be created beside it
	require.NoError(t, s.Push(context.Background(), "media", store.Work{Key: "/c", Kind: store.KindIndex, After: time.Now().Add(time.Hour)}))
	_, err = listed(t, i, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "c"}})
	require.NoError(t, err)
	assert.Equal(t, before+4, l.lists.Load(), "a file of its path")

	// As is a listing of a volume whose files aren't indexed
	folders, err := indexer.New("media", l, newStore(t), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, folders)
	before = l.lists.Load()
	_, err = listed(t, folders, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	require.NoError(t, err)
	assert.Equal(t, before+1, l.lists.Load(), "its files aren't indexed")
}

// slow is a backend which can't be watched, whose files are slow to get,
// which counts them
type slow struct {
	gofiler.Filer
	gets atomic.Int32
}

func (s *slow) WatchObjects(context.Context, chan<- schema.Event) error {
	return gofiler.ErrNotImplemented.With("it can't be watched")
}

func (s *slow) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	object, err := s.Filer.GetObject(ctx, req)
	if err == nil && !object.IsDir {
		s.gets.Add(1)
		time.Sleep(5 * time.Millisecond)
	}
	return object, err
}

func Test_ScanPages(t *testing.T) {
	backend, dir := newBackend(t)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "many"), 0o755))
	for n := range 250 {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "many", fmt.Sprintf("%03d.txt", n)), nil, 0o644))
	}
	const files = 252 // and x.txt, and a/y.txt
	s := newStore(t)
	state := func() schema.IndexState {
		t.Helper()
		state, err := s.State(context.Background(), "media")
		require.NoError(t, err)
		return state
	}
	start := func(b gofiler.Filer) (context.CancelFunc, chan error) {
		i, err := indexer.New("media", b, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour))
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- i.Run(ctx, nil) }()
		return cancel, done
	}

	// A first scan stores its files a page at a time, which its state
	// counts, while it's scanning
	first := &slow{Filer: backend}
	cancel, done := start(first)
	require.Eventually(t, func() bool {
		state := state()
		return state.Status == schema.IndexStatusScanning && state.Phase == schema.IndexPhaseIndexing && state.Files >= 100
	}, 10*time.Second, time.Millisecond, "its files are stored while it's indexing them, once they're listed")
	assert.Equal(t, uint64(5), state().Folders, "of the folders it's listed")
	cancel()
	require.NoError(t, <-done)
	stored := len(indexed(t, s))
	assert.Less(t, stored, files, "it stopped before it got them all")

	// One which stops has stored those it's got, which the next scan doesn't
	// get again
	second := &slow{Filer: backend}
	cancel, done = start(second)
	defer func() {
		cancel()
		require.NoError(t, <-done)
	}()
	require.Eventually(t, func() bool {
		state := state()
		return state.Status == schema.IndexStatusReady && state.Files == files && state.Phase == schema.IndexPhaseIdle
	}, 10*time.Second, time.Millisecond, "it's scanned, and idle")
	assert.Equal(t, int32(files-stored), second.gets.Load(), "only the files which weren't stored are got")
	assert.Len(t, indexed(t, s), files)
}

// flaky is a backend which can't be watched, whose files of the paths are
// gone, once they're listed, or fail to be got, at the attempt
type flaky struct {
	gofiler.Filer
	mu    sync.Mutex
	gone  map[string]bool
	fail  map[string]int // The attempt, from one, which fails, of a path
	tries map[string]int
}

func (f *flaky) WatchObjects(context.Context, chan<- schema.Event) error {
	return gofiler.ErrNotImplemented.With("it can't be watched")
}

func (f *flaky) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	f.mu.Lock()
	if f.tries == nil {
		f.tries = make(map[string]int)
	}
	f.tries[req.Path]++
	gone, fail := f.gone[req.Path], f.fail[req.Path] == f.tries[req.Path]
	f.mu.Unlock()
	switch {
	case gone:
		return nil, &iofs.PathError{Op: "get", Path: req.Path, Err: iofs.ErrNotExist}
	case fail:
		return nil, gofiler.ErrServiceUnavailable.With("it failed")
	}
	return f.Filer.GetObject(ctx, req)
}

func Test_ScanCountsStored(t *testing.T) {
	backend, _ := newBackend(t)
	s := newStore(t)

	// A file which is gone, once it's listed, isn't stored, so it isn't
	// counted
	f := &flaky{Filer: backend, gone: map[string]bool{"/x.txt": true}}
	i, err := indexer.New("media", f, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, i)
	state, err := s.State(context.Background(), "media")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), state.Files, "only /a/y.txt")
	assert.Equal(t, []string{"/a/y.txt"}, indexed(t, s))
}

func Test_RefreshRetry(t *testing.T) {
	backend, _ := newBackend(t)
	s := newStore(t)

	// A file of new folders, which fails to be got, as its folder is
	// indexed, is retried, with the folders above it, which aren't
	// indexed, until its folder's files are got
	f := &flaky{Filer: backend, fail: map[string]int{"/new/deep/f.txt": 2}}
	i, err := indexer.New("media", f, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour), indexer.WithDebounce(0), indexer.WithRetry(10*time.Millisecond))
	require.NoError(t, err)
	run(t, i)
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/new/deep/f.txt"}, Body: strings.NewReader("f")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, fileErr := s.GetObject(context.Background(), "media", "/new/deep/f.txt")
		_, folderErr := s.GetObject(context.Background(), "media", "/new/")
		root, rootErr := s.GetObject(context.Background(), "media", "/")
		return fileErr == nil && folderErr == nil && rootErr == nil && count(t, root) == "4"
	}, 10*time.Second, 10*time.Millisecond, "the folders above it are indexed, and the root's count")
}

func Test_CatchUp(t *testing.T) {
	backend, _ := newBackend(t)
	l := &listing{Filer: backend}
	s := newStore(t)
	i, err := indexer.New("media", l, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, i)
	x := schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/x.txt"}}
	root := schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}

	// While a rescan of the root, which is a catch-up, such as after a
	// restart, is queued, the index is served
	require.NoError(t, s.Push(context.Background(), "media", store.Work{Key: "/", Kind: store.KindRescan, After: time.Now().Add(time.Hour)}))
	gets, lists := l.gets.Load(), l.lists.Load()
	_, err = i.GetObject(context.Background(), x)
	require.NoError(t, err)
	_, err = listed(t, i, root)
	require.NoError(t, err)
	assert.Equal(t, gets, l.gets.Load(), "a file is the index's")
	assert.Equal(t, lists, l.lists.Load(), "a listing is the index's")

	// But a change, which is queued while it is, isn't of it, so its file,
	// and the listings of it, are the backend's
	require.NoError(t, s.Push(context.Background(), "media", store.Work{Key: "/x.txt", Kind: store.KindIndex, After: time.Now().Add(time.Hour)}))
	_, err = i.GetObject(context.Background(), x)
	require.NoError(t, err)
	_, err = listed(t, i, root)
	require.NoError(t, err)
	assert.Equal(t, gets+1, l.gets.Load(), "a changed file is the backend's")
	assert.Equal(t, lists+1, l.lists.Load(), "a listing of it is the backend's")
}

func Test_CatchUpChanges(t *testing.T) {
	backend, dir := newBackend(t)
	s := newStore(t)
	start := func(b gofiler.Filer) (*indexer.Indexer, func()) {
		i, err := indexer.New("media", b, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour))
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- i.Run(ctx, nil) }()
		return i, func() {
			cancel()
			require.NoError(t, <-done)
		}
	}
	scanned := func(since time.Time) bool {
		state, err := s.State(context.Background(), "media")
		return err == nil && state.Status == schema.IndexStatusReady && state.Scanned.After(since)
	}

	// A volume which is indexed, and then changed while it isn't running
	_, stop := start(&listing{Filer: backend})
	require.Eventually(t, func() bool { return scanned(time.Time{}) }, 10*time.Second, 5*time.Millisecond)
	stop()
	stopped := time.Now()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "x.txt"), []byte("changed"), 0o644))
	require.NoError(t, os.Remove(filepath.Join(dir, "a", "y.txt")))
	require.NoError(t, os.Remove(filepath.Join(dir, "a", "b")))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "e", "f"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "e", "f", "g.txt"), []byte("g"), 0o644))

	// Catches up, when it runs again, of what was added, changed, and
	// removed, getting only the files which are new, or changed
	l := &listing{Filer: backend}
	i, stop := start(l)
	defer stop()
	require.Eventually(t, func() bool { return scanned(stopped) }, 10*time.Second, 5*time.Millisecond, "it's scanned again")
	req := schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}
	want, err := listed(t, backend, req)
	require.NoError(t, err)
	got, err := listed(t, i, req)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, []string{"/e/f/g.txt", "/x.txt"}, indexed(t, s))
	x, err := s.GetObject(context.Background(), "media", "/x.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(len("changed")), *x.Size, "of the changed file")
	assert.Equal(t, int32(2), l.gets.Load(), "only /x.txt, and /e/f/g.txt, are got")
}

func Test_RefreshFolderRetry(t *testing.T) {
	backend, dir := newBackend(t)
	s := newStore(t)

	// A folder which is new, whose folder above it fails to be indexed, is
	// retried, with the folders above it, rather than being rescanned on its
	// own, as queueing its rescan replaces the work which would be retried
	f := &flaky{Filer: backend, fail: map[string]int{"/new/": 1}}
	i, err := indexer.New("media", f, s, indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour), indexer.WithDebounce(0), indexer.WithRetry(10*time.Millisecond))
	require.NoError(t, err)
	run(t, i)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "new", "deep"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "new", "deep", "f.txt"), []byte("f"), 0o644))
	require.NoError(t, s.Push(context.Background(), "media", store.Work{Key: "/new/deep/", Kind: store.KindIndex}))
	require.Eventually(t, func() bool {
		_, fileErr := s.GetObject(context.Background(), "media", "/new/deep/f.txt")
		_, folderErr := s.GetObject(context.Background(), "media", "/new/")
		root, rootErr := s.GetObject(context.Background(), "media", "/")
		return fileErr == nil && folderErr == nil && rootErr == nil && count(t, root) == "4"
	}, 10*time.Second, 10*time.Millisecond, "the folders above it are indexed, and it's rescanned")
}

// sizer is a task which stores the size of a file, as test:size, and
// counts its calls, of each file, and fails of the files it's told to
type sizer struct {
	mu    sync.Mutex
	calls map[string]int
	fail  map[string]bool
	bad   map[string]bool // The files which can't be read, as they aren't valid
	gone  map[string]bool // The files which are gone, once they're read

	// The files whose backend is unavailable, how many times, before
	// they're read
	unavailable map[string]int
}

func (z *sizer) task(ctx context.Context, _ gofiler.Filer, s indexer.MetadataStore, object *schema.Object) error {
	z.mu.Lock()
	if z.calls == nil {
		z.calls = make(map[string]int)
	}
	z.calls[object.Path]++
	fail, bad, gone := z.fail[object.Path], z.bad[object.Path], z.gone[object.Path]
	unavailable := z.calls[object.Path] <= z.unavailable[object.Path]
	z.mu.Unlock()
	if unavailable {
		return gofiler.ErrServiceUnavailable.With("read: the server is busy")
	} else if bad {
		return gofiler.ErrBadParameter.With("not a valid file")
	} else if fail {
		return errors.New("it can't be read")
	} else if gone {
		return &iofs.PathError{Op: "read", Path: object.Path, Err: iofs.ErrNotExist}
	}
	return s.SetMetadata(ctx, object.Path, schema.TaskMeta{Meta: []schema.Meta{{Namespace: "test", Key: "size", Value: json.RawMessage(fmt.Sprint(*object.Size))}}})
}

func (z *sizer) count(path string) int {
	z.mu.Lock()
	defer z.mu.Unlock()
	return z.calls[path]
}

func Test_MetadataTask(t *testing.T) {
	backend, dir := newBackend(t)
	s := newStore(t)
	z := &sizer{fail: map[string]bool{"/a/y.txt": true}}
	i, err := indexer.New("media", &unwatched{Filer: backend}, s, indexer.WithMode(indexer.ModeMetadata), indexer.WithMetadataTask(z.task), indexer.WithRescanInterval(time.Hour), indexer.WithDebounce(0), indexer.WithRetry(time.Millisecond))
	require.NoError(t, err)
	run(t, i)
	size := func(path string) string {
		object, err := s.GetObject(context.Background(), "media", path)
		if err != nil {
			return ""
		}
		for _, m := range object.Meta {
			if m.Namespace == "test" && m.Key == "size" {
				return string(m.Value)
			}
		}
		return ""
	}

	// Each file which is indexed is enriched, of the task, once, and one
	// which fails is tried a few times, and given up
	require.Eventually(t, func() bool { return size("/x.txt") == "5" }, 10*time.Second, 5*time.Millisecond, "it's enriched")
	require.Eventually(t, func() bool { return z.count("/a/y.txt") == 5 }, 10*time.Second, 5*time.Millisecond, "it's given up, after 5 attempts")
	assert.Equal(t, 1, z.count("/x.txt"))

	// Its state is of the files which are indexed, and failed, with why
	require.Eventually(t, func() bool {
		state, err := i.Metadata(context.Background())
		return err == nil && state != nil && state.Queued == 0 && state.Indexed == 2 && state.Failed == 1
	}, 10*time.Second, 5*time.Millisecond, "it's indexed, of one which failed")
	state, err := i.Metadata(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "it can't be read", state.Error)

	// A rescan of files which haven't changed doesn't enrich them again, but
	// one which has changed is, and one which was given up is tried again,
	// once it changes
	require.NoError(t, s.Push(context.Background(), "media", store.Work{Key: "/", Kind: store.KindRescan}))
	require.Eventually(t, func() bool {
		work, err := s.Claim(context.Background(), "media", []store.Kind{store.KindRescan}, 1, time.Millisecond)
		return err == nil && len(work) == 0
	}, 10*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, z.count("/x.txt"), "it hasn't changed")
	assert.Equal(t, 5, z.count("/a/y.txt"), "it hasn't changed")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "x.txt"), []byte("changed"), 0o644))
	require.NoError(t, s.Push(context.Background(), "media", store.Work{Key: "/", Kind: store.KindRescan}))
	require.Eventually(t, func() bool { return size("/x.txt") == "7" }, 10*time.Second, 5*time.Millisecond, "it's enriched again")
	assert.Equal(t, 2, z.count("/x.txt"))

	// A volume whose files are indexed, but not enriched, doesn't run it
	objects := &sizer{}
	o, err := indexer.New("media", &unwatched{Filer: backend}, newStore(t), indexer.WithMode(indexer.ModeObjects), indexer.WithMetadataTask(objects.task), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, o)
	time.Sleep(100 * time.Millisecond)
	assert.Zero(t, objects.count("/x.txt"), "of index=objects")
}

// syncBuffer is a buffer which is written from more than one goroutine
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func Test_MetadataTaskUnreadable(t *testing.T) {
	backend, _ := newBackend(t)
	z := &sizer{bad: map[string]bool{"/a/y.txt": true}}
	i, err := indexer.New("media", &unwatched{Filer: backend}, newStore(t), indexer.WithMode(indexer.ModeMetadata), indexer.WithMetadataTask(z.task), indexer.WithRescanInterval(time.Hour), indexer.WithRetry(time.Millisecond))
	require.NoError(t, err)
	var logs syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- i.Run(ctx, slog.New(slog.NewTextHandler(&logs, nil))) }()
	defer func() {
		cancel()
		require.NoError(t, <-done)
	}()

	// A file which can't be read is given up at once, rather than tried
	// again, and is failed, with why, which is logged, of its volume
	require.Eventually(t, func() bool {
		state, err := i.Metadata(context.Background())
		return err == nil && state != nil && state.Queued == 0 && state.Failed == 1
	}, 10*time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, z.count("/a/y.txt"), "it isn't tried again")
	state, err := i.Metadata(context.Background())
	require.NoError(t, err)
	assert.Contains(t, state.Error, "not a valid file")
	assert.Contains(t, logs.String(), "level=WARN")
	assert.Contains(t, logs.String(), "volume=media")
	assert.Contains(t, logs.String(), "key=/a/y.txt")
	assert.Contains(t, logs.String(), "not a valid file")
}

// A file whose backend is unavailable, for a while, isn't given up, as
// it's tried again, until it's read
func Test_MetadataTaskUnavailable(t *testing.T) {
	backend, _ := newBackend(t)
	s := newStore(t)
	z := &sizer{unavailable: map[string]int{"/a/y.txt": 8}}
	i, err := indexer.New("media", &unwatched{Filer: backend}, s, indexer.WithMode(indexer.ModeMetadata), indexer.WithMetadataTask(z.task), indexer.WithRescanInterval(time.Hour), indexer.WithRetry(time.Millisecond))
	require.NoError(t, err)
	run(t, i)
	require.Eventually(t, func() bool {
		state, err := i.Metadata(context.Background())
		return err == nil && state != nil && state.Queued == 0 && state.Indexed == 2
	}, 30*time.Second, 5*time.Millisecond, "it's enriched, once it's read")
	assert.Equal(t, 9, z.count("/a/y.txt"))
	state, err := i.Metadata(context.Background())
	require.NoError(t, err)
	assert.Zero(t, state.Failed)
}

func Test_MetadataTaskUpgrade(t *testing.T) {
	backend, _ := newBackend(t)
	s := newStore(t)
	start := func(opts ...indexer.Opt) func() {
		i, err := indexer.New("media", &unwatched{Filer: backend}, s, append([]indexer.Opt{indexer.WithRescanInterval(time.Hour)}, opts...)...)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- i.Run(ctx, nil) }()
		return func() {
			cancel()
			require.NoError(t, <-done)
		}
	}

	// A volume whose files are indexed
	stop := start(indexer.WithMode(indexer.ModeObjects))
	files(t, s, "/a/y.txt", "/x.txt")
	require.Eventually(t, func() bool {
		state, err := s.State(context.Background(), "media")
		return err == nil && state.Status == schema.IndexStatusReady && state.Files == 2
	}, 10*time.Second, 5*time.Millisecond)

	// One of which is stored again, as a watch event does when it changes,
	// so its listing's revision isn't, until it's scanned again
	object, err := s.GetObject(context.Background(), "media", "/x.txt")
	require.NoError(t, err)
	require.NoError(t, s.PutObject(context.Background(), "media", object))
	revisions, err := s.Revisions(context.Background(), "media", "/", true)
	require.NoError(t, err)
	require.NotContains(t, revisions, "/x.txt")
	require.Contains(t, revisions, "/a/y.txt")
	stop()

	// Is enriched, once it's of index=metadata, at once, rather than once
	// it's scanned again, which can be slow, as its listing is here
	z := &sizer{}
	g := &gated{Filer: backend, release: make(chan struct{})}
	i, err := indexer.New("media", &unwatched{Filer: g}, s, indexer.WithRescanInterval(time.Hour), indexer.WithMode(indexer.ModeMetadata), indexer.WithMetadataTask(z.task))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- i.Run(ctx, nil) }()
	defer func() {
		cancel()
		require.NoError(t, <-done)
	}()
	require.Eventually(t, func() bool { return z.count("/x.txt") == 1 && z.count("/a/y.txt") == 1 }, 10*time.Second, 5*time.Millisecond, "its files are enriched, while it's scanned, as is one without a revision")
	close(g.release)
}

// gated is a backend whose listings wait until it's released, or they're
// cancelled, as a slow volume's do
type gated struct {
	gofiler.Filer
	release chan struct{}
}

func (g *gated) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	select {
	case <-g.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return g.Filer.ListObjects(ctx, req)
}

func Test_MetadataTaskDowngrade(t *testing.T) {
	backend, _ := newBackend(t)
	s := newStore(t)
	start := func(z *sizer, mode indexer.Mode) func() {
		i, err := indexer.New("media", &unwatched{Filer: backend}, s, indexer.WithRescanInterval(time.Hour), indexer.WithMode(mode), indexer.WithMetadataTask(z.task))
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- i.Run(ctx, nil) }()
		return func() {
			cancel()
			require.NoError(t, <-done)
		}
	}
	enriched := func() bool {
		object, err := s.GetObject(context.Background(), "media", "/x.txt")
		if err != nil {
			return false
		}
		for _, m := range object.Meta {
			if m.Namespace == "test" {
				return true
			}
		}
		return false
	}

	// A volume which is enriched, and then isn't, of index=objects, doesn't
	// keep what it was enriched with, and is enriched again, of every file,
	// when it's of index=metadata again
	z := &sizer{}
	stop := start(z, indexer.ModeMetadata)
	require.Eventually(t, enriched, 10*time.Second, 5*time.Millisecond, "it's enriched")
	stop()
	stop = start(&sizer{}, indexer.ModeObjects)
	require.Eventually(t, func() bool { return !enriched() }, 10*time.Second, 5*time.Millisecond, "what it was enriched with is removed")
	stop()
	stop = start(z, indexer.ModeMetadata)
	defer stop()
	require.Eventually(t, func() bool { return enriched() && z.count("/x.txt") == 2 }, 10*time.Second, 5*time.Millisecond, "it's enriched again")
}

func Test_MetadataTaskGone(t *testing.T) {
	backend, _ := newBackend(t)
	z := &sizer{gone: map[string]bool{"/x.txt": true}}
	i, err := indexer.New("media", &unwatched{Filer: backend}, newStore(t), indexer.WithMode(indexer.ModeMetadata), indexer.WithMetadataTask(z.task), indexer.WithRescanInterval(time.Hour), indexer.WithRetry(time.Millisecond))
	require.NoError(t, err)
	run(t, i)

	// A file which is gone, as the backend says, once it's read, is done,
	// rather than tried again
	require.Eventually(t, func() bool { return z.count("/a/y.txt") == 1 && z.count("/x.txt") == 1 }, 10*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, z.count("/x.txt"), "it isn't tried again")
}

func Test_Poll(t *testing.T) {
	backend, dir := newBackend(t)

	// A backend which can't be watched is scanned, and scanned again, at
	// the rescan interval
	i, err := indexer.New("media", &unwatched{Filer: backend}, newStore(t), indexer.WithRescanInterval(20*time.Millisecond), indexer.WithRescanMinInterval(0))
	require.NoError(t, err)
	run(t, i)
	assert.Equal(t, []string{"/a", "/a/b", "/c"}, folders(t, i, "/", true))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "d"), 0o755))
	eventually(t, i, "/a", "/a/b", "/c", "/d")
}

func Test_Retry(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, _ := newBackend(t)

	// A watch which fails is watched again, after a while, with why it
	// failed, until it's scanned again
	failing := &unwatched{Filer: backend, fail: true}
	s := newStore(t)
	i, err := indexer.New("media", failing, s, indexer.WithRetry(10*time.Millisecond))
	require.NoError(t, err)
	run(t, i)
	assert.GreaterOrEqual(t, failing.watches.Load(), int32(2))
	assert.Equal(t, []string{"/a", "/a/b", "/c"}, folders(t, i, "/", true))
	state, err := i.State(context.Background())
	require.NoError(t, err)
	assert.Empty(t, state.Error, "the scan once it's watched clears it")
}

func Test_Symlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symbolic links need privileges on Windows")
	}
	backend, dir := newBackend(t)
	require.NoError(t, os.Symlink(".", filepath.Join(dir, "a", "loop")))

	// A link to a directory isn't an object, as a bucket has no links, so
	// it isn't a folder, and isn't descended into
	i, err := indexer.New("media", &unwatched{Filer: backend}, newStore(t), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, i)
	assert.Equal(t, []string{"/a", "/a/b", "/c"}, folders(t, i, "/", true))
}

func Test_DeleteParents(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, _ := newBackend(t)
	i, err := indexer.New("media", backend, newStore(t))
	require.NoError(t, err)
	run(t, i)

	// The last file of folders, which the backend removes, when it's
	// deleted, removes them from the index, and the root's count changes
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/p/q/z.txt"}, Body: strings.NewReader("z")})
	require.NoError(t, err)
	eventually(t, i, "/a", "/a/b", "/c", "/p", "/p/q")
	_, err = i.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/p/q/z.txt"}})
	require.NoError(t, err)
	eventually(t, i, "/a", "/a/b", "/c")
	require.Eventually(t, func() bool {
		root, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
		return err == nil && count(t, root) == "3"
	}, 10*time.Second, 10*time.Millisecond, "a, c and x.txt")
}

func Test_Bucket(t *testing.T) {
	// A bucket, whose file, /a, is beside a folder of the same name, /a/
	b := newBucket("a", "a/b/c.txt", "x.txt")
	i, err := indexer.New("media", b, newStore(t), indexer.WithRescanInterval(time.Hour), indexer.WithDebounce(0))
	require.NoError(t, err)
	run(t, i)
	assert.Equal(t, []string{"/a", "/a/b"}, folders(t, i, "/", true))

	// The path of the file is the backend's, which is the file, and the
	// folder's, with a "/" after it, is the index's
	object, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a"}})
	require.NoError(t, err)
	assert.False(t, object.IsDir, "the file")
	object, err = i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a/"}})
	require.NoError(t, err)
	assert.True(t, object.IsDir, "the folder")

	// Writing the file doesn't remove the folder beside it, with what's
	// beneath it
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a"}, Body: strings.NewReader("a")})
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, []string{"/a", "/a/b"}, folders(t, i, "/", true))

	// Deleting the folder's last file removes it, but not the file
	_, err = i.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a/b/c.txt"}})
	require.NoError(t, err)
	eventually(t, i)
	object, err = i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a"}})
	require.NoError(t, err)
	assert.False(t, object.IsDir)
}

func Test_RunStuckWatcher(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	backend, dir := newBackend(t)
	i, err := indexer.New("media", backend, newStore(t))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- i.Run(ctx, nil) }()

	// A watcher which doesn't read its changes, but isn't cancelled
	stuck := make(chan schema.Event)
	watchCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() { _ = i.WatchObjects(watchCtx, stuck) }()
	require.Eventually(t, func() bool {
		state, err := i.State(context.Background())
		return err == nil && state.Status == schema.IndexStatusReady
	}, 10*time.Second, 5*time.Millisecond)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "d"), 0o755))
	time.Sleep(100 * time.Millisecond)

	// Cancelling Run returns, though the watcher isn't reading
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run waited for a watcher which doesn't read its changes")
	}
}

// newStore returns a store in memory, with the volume media, which the
// registry puts before it indexes a volume
func newStore(t *testing.T) *sqlite.Store {
	t.Helper()
	s, err := sqlite.New(context.Background(), gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	_, err = s.CreateVolume(context.Background(), schema.VolumeMeta{Name: "media", URL: "file://media/tmp?index=folders", Enabled: true})
	require.NoError(t, err)
	return s
}

// watchDown is a backend whose watches fail, as its server is down
type watchDown struct {
	gofiler.Filer
}

func (w watchDown) WatchObjects(context.Context, chan<- schema.Event) error {
	return gofiler.ErrServiceUnavailable.With("server is down")
}

func Test_IndexingFailedLogs(t *testing.T) {
	backend, _ := newBackend(t)
	i, err := indexer.New("media", watchDown{Filer: backend}, newStore(t), indexer.WithRetry(time.Millisecond))
	require.NoError(t, err)
	var logs syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- i.Run(ctx, slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}()
	defer func() {
		cancel()
		require.NoError(t, <-done)
	}()

	// Watching which fails again, of the same reason, is a warning once, and
	// then for debugging, as its state says why it failed
	require.Eventually(t, func() bool { return strings.Count(logs.String(), "indexing failed, again") >= 3 }, 10*time.Second, time.Millisecond)
	assert.Equal(t, 1, strings.Count(logs.String(), "level=WARN msg=\"indexing failed\""))
}
