package indexer_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fs "github.com/mutablelogic/go-filer/filer/fs"
	fsevents "github.com/mutablelogic/go-filer/filer/fs/fsevents"
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	store "github.com/mutablelogic/go-filer/filer/store"
	sqlite "github.com/mutablelogic/go-filer/filer/store/sqlite"
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
	go func() { done <- i.Run(ctx) }()
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
	indexed, err := s.Get(context.Background(), "media", "/a")
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
	objects, err := s.List(context.Background(), "media", store.ListRequest{Parent: "/", Recursive: true})
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
	file, err := s.Get(context.Background(), "media", "/x.txt")
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
		file, err := s.Get(context.Background(), "media", "/x.txt")
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
	go func() { done <- i.Run(ctx) }()
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

func (f *faultyStore) Get(ctx context.Context, volume, path string) (*schema.Object, error) {
	if f.path != "" && path == f.path {
		return nil, gofiler.ErrInternalServerError.With("the read failed")
	}
	return f.Store.Get(ctx, volume, path)
}

func (f *faultyStore) List(ctx context.Context, volume string, req store.ListRequest) ([]*schema.Object, error) {
	if f.lists.Add(-1) >= 0 {
		return nil, gofiler.ErrInternalServerError.With("the listing failed")
	}
	return f.Store.List(ctx, volume, req)
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
		object, err := s.Get(context.Background(), "media", path)
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
	// again
	b := newBucket("a", "x.txt")
	i, err := indexer.New("media", b, newStore(t), indexer.WithMode(indexer.ModeObjects), indexer.WithRescanInterval(time.Hour), indexer.WithDebounce(0))
	require.NoError(t, err)
	run(t, i)
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a/b/c.txt"}, Body: strings.NewReader("c")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		root, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
		return err == nil && count(t, root) == "3"
	}, 10*time.Second, 10*time.Millisecond, "a, a/ and x.txt")
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
	time.Sleep(100 * time.Millisecond)
	n, object := gets(t, i, c, x)
	assert.Zero(t, n, "it's the index's")
	assert.Equal(t, "media", object.Name)
	assert.Equal(t, int64(len("x.txt")), *object.Size)

	// But not a file which isn't indexed, a request which is conditional, or
	// a file which has work queued, which would change it
	n, _ = gets(t, i, c, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a/"}})
	assert.Zero(t, n, "a folder is the index's")
	n, _ = gets(t, i, c, schema.GetObjectRequest{ObjectKey: x.ObjectKey, IfModifiedSince: time.Unix(1, 0)})
	assert.Equal(t, int32(1), n, "a conditional request is the backend's")
	require.NoError(t, s.Push(context.Background(), "media", store.Work{Path: "/x.txt", Kind: store.KindIndex, After: time.Now().Add(time.Hour)}))
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
	go func() { done <- i.Run(ctx) }()

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
