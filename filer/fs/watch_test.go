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
			t.Fatalf("no change to %q, but %v", path, result)
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

	t.Run("BrokenLink", func(t *testing.T) {
		// A link which is broken isn't listed, so creating it isn't sent
		dir, events := watchVolume(t)
		if err := os.Symlink(filepath.Join(dir, "missing"), filepath.Join(dir, "broken")); err != nil {
			t.Skipf("can't create a link: %v", err)
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, "c.txt"), nil, 0o600))
		for _, event := range receive(t, events, "/c.txt") {
			assert.Equal(t, "/c.txt", event.Path)
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
