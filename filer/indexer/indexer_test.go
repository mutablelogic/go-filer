package indexer_test

import (
	"context"
	"errors"
	"io"
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
	// it, is indexed as it's written, with the count of the folder's
	// objects
	_, err = i.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/f/g/z.txt"}, Body: strings.NewReader("z")})
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/a/b", "/c", "/f", "/f/g"}, folders(t, i, "/", true))
	object, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/f/g/"}})
	require.NoError(t, err)
	assert.Equal(t, "1", count(t, object))

	// A folder which is deleted through it, with what's beneath it, is
	// removed from the index as it's deleted
	_, err = i.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a/"}, Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"/c", "/f", "/f/g"}, folders(t, i, "/", true))
}

func Test_Poll(t *testing.T) {
	backend, dir := newBackend(t)

	// A backend which can't be watched is scanned, and scanned again, at
	// the rescan interval
	i, err := indexer.New("media", &unwatched{Filer: backend}, newStore(t), indexer.WithRescanInterval(20*time.Millisecond))
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

	// A link to a directory, which the backend lists, but doesn't descend
	// into, as it could loop, is a folder, which isn't descended into
	i, err := indexer.New("media", &unwatched{Filer: backend}, newStore(t), indexer.WithRescanInterval(time.Hour))
	require.NoError(t, err)
	run(t, i)
	assert.Equal(t, []string{"/a", "/a/b", "/a/loop", "/c"}, folders(t, i, "/", true))
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
	assert.Equal(t, []string{"/a", "/a/b", "/c", "/p", "/p/q"}, folders(t, i, "/", true))
	_, err = i.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/p/q/z.txt"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/a/b", "/c"}, folders(t, i, "/", true))
	root, err := i.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	require.NoError(t, err)
	assert.Equal(t, "3", count(t, root), "a, c and x.txt")
}

func Test_Bucket(t *testing.T) {
	// A bucket, whose file, /a, is beside a folder of the same name, /a/
	b := newBucket("a", "a/b/c.txt", "x.txt")
	i, err := indexer.New("media", b, newStore(t), indexer.WithRescanInterval(time.Hour))
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
	assert.Equal(t, []string{"/a", "/a/b"}, folders(t, i, "/", true))

	// Deleting the folder's last file removes it, but not the file
	_, err = i.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a/b/c.txt"}})
	require.NoError(t, err)
	assert.Empty(t, folders(t, i, "/", true))
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
