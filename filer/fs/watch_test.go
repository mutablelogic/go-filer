package fs

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	fsevents "github.com/mutablelogic/go-filer/filer/fs/fsevents"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// watchVolume watches a new volume, and returns its directory and the
// changes, once it's watched
func watchVolume(t *testing.T) (string, chan schema.Event) {
	t.Helper()
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	dir := t.TempDir()
	backend, err := New("test", dir, nil, filer.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	events, done := make(chan schema.Event, 1024), make(chan error, 1)
	go func() {
		done <- backend.WatchObjects(ctx, events)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("watching didn't stop")
		}
	})

	// Write a file until its change is received, so changes are watched
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "probe.txt"), nil, 0o600))
		select {
		case event := <-events:
			if event.Path == "/probe.txt" {
				time.Sleep(200 * time.Millisecond)
				for len(events) > 0 {
					<-events
				}
				return dir, events
			}
		case err := <-done:
			t.Fatalf("watching stopped: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatal("the volume wasn't watched")
	return "", nil
}

// receive returns the changes until one for the path is received
func receive(t *testing.T, events chan schema.Event, path string) []schema.Event {
	t.Helper()
	var result []schema.Event
	for timeout := time.After(10 * time.Second); ; {
		select {
		case event := <-events:
			result = append(result, event)
			if event.Path == path {
				return result
			}
		case <-timeout:
			paths := []string{}
			for _, e := range result {
				paths = append(paths, e.Path)
			}
			t.Fatalf("no change to %q, but %v %v", path, result, paths)
		}
	}
}

func Test_WatchObjects(t *testing.T) {
	t.Run("Objects", func(t *testing.T) {
		// Changes are to objects in the volume, with their paths
		dir, events := watchVolume(t)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o600))
		received := receive(t, events, "/a.txt")
		assert.Equal(t, schema.Event{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Path: "/a.txt"}}, received[len(received)-1])

		require.NoError(t, os.Remove(filepath.Join(dir, "a.txt")))
		for {
			if event := receive(t, events, "/a.txt"); event[len(event)-1].EventType == schema.EventTypeDeleted {
				break
			}
		}
	})

	t.Run("Folder", func(t *testing.T) {
		// A folder which is created is of its key, with a "/" after its
		// path, and one which is deleted is of its path, as it can't be
		// checked
		dir, events := watchVolume(t)
		require.NoError(t, os.Mkdir(filepath.Join(dir, "d"), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "d", "e.txt"), []byte("e"), 0o600))
		receive(t, events, "/d/")
		require.NoError(t, os.RemoveAll(filepath.Join(dir, "d")))
		for {
			if event := receive(t, events, "/d"); event[len(event)-1].EventType == schema.EventTypeDeleted {
				break
			}
		}
	})

	t.Run("Skipped", func(t *testing.T) {
		// Changes to files which aren't listed aren't sent
		dir, events := watchVolume(t)
		require.NoError(t, os.WriteFile(filepath.Join(dir, ".hidden"), nil, 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "backup~"), nil, 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "b.txt"), nil, 0o600))
		for _, event := range receive(t, events, "/b.txt") {
			assert.Equal(t, "/b.txt", event.Path)
		}
	})

	t.Run("Package", func(t *testing.T) {
		// A change within a package is an update to the package
		dir, events := watchVolume(t)
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "Disk.sparsebundle", "bands"), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "Disk.sparsebundle", "bands", "0"), []byte("band"), 0o600))
		for {
			received := receive(t, events, "/Disk.sparsebundle")
			if received[len(received)-1].EventType == schema.EventTypeUpdated {
				break
			}
		}
	})

	t.Run("PackageHidden", func(t *testing.T) {
		// A change to a hidden file within a package is an update to the
		// package
		dir, events := watchVolume(t)
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "App.bundle"), 0o700))
		receive(t, events, "/App.bundle")
		require.NoError(t, os.WriteFile(filepath.Join(dir, "App.bundle", ".state"), []byte("state"), 0o600))
		for {
			received := receive(t, events, "/App.bundle")
			if received[len(received)-1].EventType == schema.EventTypeUpdated {
				break
			}
		}
	})

	t.Run("Link", func(t *testing.T) {
		// A link isn't an object, so creating one is a deletion of its path,
		// as it can replace an object, such as a link renamed over a file
		dir, events := watchVolume(t)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "c.txt"), []byte("c"), 0o600))
		receive(t, events, "/c.txt")
		if err := os.Symlink("missing", filepath.Join(dir, ".staged")); err != nil {
			t.Skipf("can't create a link: %v", err)
		}
		require.NoError(t, os.Rename(filepath.Join(dir, ".staged"), filepath.Join(dir, "c.txt")))
		for {
			if received := receive(t, events, "/c.txt"); received[len(received)-1].EventType == schema.EventTypeDeleted {
				break
			}
		}
	})

	t.Run("NotDirectory", func(t *testing.T) {
		// A filesystem which isn't a directory can't be watched
		backend, err := NewFS("test", fstest.MapFS{}, nil, filer.Opts{})
		require.NoError(t, err)
		assert.ErrorIs(t, backend.WatchObjects(context.Background(), make(chan schema.Event)), filer.ErrNotImplemented)
	})
}

func Test_WatchObjectsClosed(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	self, err := New("test", t.TempDir(), nil, filer.Opts{})
	require.NoError(t, err)

	// Closing the filesystem stops watching, with fs.ErrClosed
	done := make(chan error, 1)
	go func() {
		done <- self.WatchObjects(context.Background(), make(chan schema.Event, 1))
	}()
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, self.Close())
	select {
	case err := <-done:
		assert.ErrorIs(t, err, fs.ErrClosed)
	case <-time.After(10 * time.Second):
		t.Fatal("watching didn't stop")
	}

	// A closed filesystem can't be watched
	assert.ErrorIs(t, self.WatchObjects(context.Background(), make(chan schema.Event)), fs.ErrClosed)
}

// Test_eventTypes checks the changes to files have the values of the changes
// to objects, as they're cast from one to the other
func Test_eventTypes(t *testing.T) {
	for op, want := range map[fsevents.Op]schema.EventType{
		fsevents.Create: schema.EventTypeCreated,
		fsevents.Update: schema.EventTypeUpdated,
		fsevents.Delete: schema.EventTypeDeleted,
		fsevents.Rescan: schema.EventTypeRescan,
	} {
		assert.Equal(t, want, schema.EventType(op), "op %d", op)
	}

	// Rescan is the last change to files, and to objects, which events are
	// checked against, so a change which is added to either fails here
	assert.Equal(t, "unknown", (fsevents.Rescan + 1).String())
	assert.Equal(t, "none", (schema.EventTypeRescan + 1).String())

	// A change which isn't known isn't sent
	self := &FS{}
	_, ok := self.event(fsevents.Event{Op: fsevents.Rescan + 1, Path: "a.txt"})
	assert.False(t, ok)
	_, ok = self.event(fsevents.Event{Path: "a.txt"})
	assert.False(t, ok)
}

func Test_WatchObjectsShared(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	dir := t.TempDir()
	backend, err := New("test", dir, nil, filer.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	before := noWatches(t)

	// watch watches the backend, until it's stopped, once its first change,
	// a rescan of the root, says it's watching
	watch := func() (chan schema.Event, func() error) {
		ctx, cancel := context.WithCancel(context.Background())
		events, done := make(chan schema.Event, 1024), make(chan error, 1)
		go func() { done <- backend.WatchObjects(ctx, events) }()
		select {
		case event := <-events:
			require.Equal(t, schema.Rescan(""), event)
		case err := <-done:
			t.Fatalf("watching failed: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("watching wasn't ready")
		}
		return events, func() error {
			cancel()
			select {
			case err := <-done:
				return err
			case <-time.After(10 * time.Second):
				t.Fatal("watching didn't stop")
				return nil
			}
		}
	}

	// Two calls share one watch of the directory, which is ready for the
	// second at once, and both are sent a change
	a, stopA := watch()
	b, stopB := watch()
	assert.Equal(t, before+1, watching.Load(), "one watch of the directory")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), nil, 0o600))
	receive(t, a, "/a.txt")
	receive(t, b, "/a.txt")

	// One leaving doesn't stop the other
	require.NoError(t, stopA())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.txt"), nil, 0o600))
	receive(t, b, "/b.txt")
	assert.Equal(t, before+1, watching.Load())

	// The last to leave stops it, and a call after starts another
	require.NoError(t, stopB())
	require.Eventually(t, func() bool { return watching.Load() == before }, 10*time.Second, 10*time.Millisecond)
	c, stopC := watch()
	assert.Equal(t, before+1, watching.Load())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "c.txt"), nil, 0o600))
	receive(t, c, "/c.txt")
	require.NoError(t, stopC())
}

func Test_WatchObjectsRejoin(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	dir := t.TempDir()
	backend, err := New("test", dir, nil, filer.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	before := noWatches(t)

	// A call which leaves, and one which then joins, at once, doesn't start
	// another watch until the last ends, so the directory is watched once
	for range 20 {
		ctx, cancel := context.WithCancel(context.Background())
		events, done := make(chan schema.Event, 16), make(chan error, 1)
		go func() { done <- backend.WatchObjects(ctx, events) }()
		select {
		case event := <-events:
			require.Equal(t, schema.Rescan(""), event)
		case <-time.After(10 * time.Second):
			t.Fatal("watching wasn't ready")
		}
		assert.Equal(t, before+1, watching.Load(), "one watch of the directory")
		cancel()
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("a call which was cancelled didn't return")
		}
	}
	require.Eventually(t, func() bool { return watching.Load() == before }, 10*time.Second, 10*time.Millisecond)

	// A call whose context is cancelled already isn't made
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, backend.WatchObjects(ctx, make(chan schema.Event)), context.Canceled)
}

// noWatches waits for the watches of the tests before, which stop after
// their calls return, such as on Linux, to end, so the watches of a test
// are counted from none
func noWatches(t *testing.T) int32 {
	t.Helper()
	require.Eventually(t, func() bool { return watching.Load() == 0 }, 10*time.Second, 10*time.Millisecond, "the watches of the tests before didn't stop")
	return 0
}
