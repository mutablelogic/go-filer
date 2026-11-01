package fsevents_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	// Packages
	fsevents "github.com/mutablelogic/go-filer/fs/fsevents"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// How long to wait for events
const timeout = 10 * time.Second

// watch is a directory which is watched, with the events received
type watch struct {
	dir    string
	events chan fsevents.Event
	cancel context.CancelFunc
	done   chan error
}

// start watches a new directory, and waits until it's watched
func start(t *testing.T) *watch {
	t.Helper()
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &watch{dir: t.TempDir(), events: make(chan fsevents.Event, 1024), cancel: cancel, done: make(chan error, 1)}
	go func() {
		w.done <- fsevents.Watch(ctx, w.dir, w.events)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-w.done:
		case <-time.After(timeout):
			t.Error("the watcher didn't stop")
		}
	})

	// Write a file until its change is received, so changes are watched
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		w.write(t, ".probe", "probe")
		for wait := time.After(100 * time.Millisecond); ; {
			select {
			case event := <-w.events:
				if event.Path == ".probe" {
					w.drain()
					return w
				}
				continue
			case err := <-w.done:
				t.Fatalf("the watcher stopped: %v", err)
			case <-wait:
			}
			break
		}
	}
	t.Fatal("the directory wasn't watched")
	return nil
}

// path returns the path of the file in the watched directory
func (w *watch) path(rel string) string {
	return filepath.Join(w.dir, filepath.FromSlash(rel))
}

// write writes the file in the watched directory
func (w *watch) write(t *testing.T, rel, data string) {
	t.Helper()
	require.NoError(t, os.WriteFile(w.path(rel), []byte(data), 0o600))
}

// drain discards the events received so far, after a moment
func (w *watch) drain() {
	time.Sleep(200 * time.Millisecond)
	for {
		select {
		case <-w.events:
		default:
			return
		}
	}
}

// expect waits for the events, in any order, among any others
func (w *watch) expect(t *testing.T, want ...fsevents.Event) {
	t.Helper()
	var seen []string
	deadline := time.After(timeout)
	for len(want) > 0 {
		select {
		case event := <-w.events:
			seen = append(seen, event.String())
			for i := range want {
				if matches(want[i], event) {
					want = append(want[:i], want[i+1:]...)
					break
				}
			}
		case err := <-w.done:
			t.Fatalf("the watcher stopped: %v", err)
		case <-deadline:
			t.Fatalf("didn't receive %v, but %v", want, seen)
		}
	}
}

// expectNone checks that there are no events for the path, or beneath it,
// for a moment
func (w *watch) expectNone(t *testing.T, rel string) {
	t.Helper()
	wait := time.After(500 * time.Millisecond)
	for {
		select {
		case event := <-w.events:
			assert.False(t, event.Path == rel || strings.HasPrefix(event.Path, rel+"/"), "unexpected %v", event)
		case <-wait:
			return
		}
	}
}

// matches returns true when the event is the one wanted. A deleted directory
// isn't known to be one on Windows.
func matches(want, event fsevents.Event) bool {
	if want.Op != event.Op || want.Path != event.Path {
		return false
	}
	return want.IsDir == event.IsDir || (want.Op == fsevents.Delete && runtime.GOOS == "windows")
}

func dir(op fsevents.Op, path string) fsevents.Event {
	return fsevents.Event{Op: op, Path: path, IsDir: true}
}

func file(op fsevents.Op, path string) fsevents.Event {
	return fsevents.Event{Op: op, Path: path}
}

func TestWatch(t *testing.T) {
	t.Run("Files", func(t *testing.T) {
		// A file is created, updated, renamed and deleted
		w := start(t)
		w.write(t, "a.txt", "hello")
		w.expect(t, file(fsevents.Create, "a.txt"))
		w.drain()

		w.write(t, "a.txt", "hello again")
		w.expect(t, file(fsevents.Update, "a.txt"))

		require.NoError(t, os.Rename(w.path("a.txt"), w.path("b.txt")))
		w.expect(t, file(fsevents.Delete, "a.txt"), file(fsevents.Create, "b.txt"))

		require.NoError(t, os.Remove(w.path("b.txt")))
		w.expect(t, file(fsevents.Delete, "b.txt"))
	})

	t.Run("Directories", func(t *testing.T) {
		// Directories, and the files in them, are created, at any depth
		w := start(t)
		require.NoError(t, os.MkdirAll(w.path("x/y/z"), 0o700))
		w.expect(t, dir(fsevents.Create, "x"))
		w.write(t, "x/y/z/c.txt", "nested")
		w.expect(t, file(fsevents.Create, "x/y/z/c.txt"))

		// A directory is deleted
		require.NoError(t, os.RemoveAll(w.path("x")))
		w.expect(t, dir(fsevents.Delete, "x"))
	})

	t.Run("MoveIn", func(t *testing.T) {
		// A directory moved in is created, with everything in it
		w := start(t)
		outside := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(outside, "in", "sub"), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(outside, "in", "sub", "d.txt"), []byte("in"), 0o600))
		require.NoError(t, os.Rename(filepath.Join(outside, "in"), w.path("in")))
		w.expect(t, dir(fsevents.Create, "in"), dir(fsevents.Create, "in/sub"), file(fsevents.Create, "in/sub/d.txt"))

		// And what's created in it later is created
		w.write(t, "in/sub/e.txt", "later")
		w.expect(t, file(fsevents.Create, "in/sub/e.txt"))
	})

	t.Run("MoveOut", func(t *testing.T) {
		// A directory moved out is deleted, and what changes in it later
		// isn't reported
		w := start(t)
		require.NoError(t, os.MkdirAll(w.path("out/sub"), 0o700))
		w.expect(t, dir(fsevents.Create, "out"))
		w.drain()
		outside := t.TempDir()
		require.NoError(t, os.Rename(w.path("out"), filepath.Join(outside, "out")))
		w.expect(t, dir(fsevents.Delete, "out"))
		require.NoError(t, os.WriteFile(filepath.Join(outside, "out", "sub", "f.txt"), []byte("out"), 0o600))
		w.expectNone(t, "out")
	})

	t.Run("Rename", func(t *testing.T) {
		// A directory renamed is deleted and created, and what's created in
		// it later has its new path
		w := start(t)
		require.NoError(t, os.MkdirAll(w.path("old/sub"), 0o700))
		w.expect(t, dir(fsevents.Create, "old"))
		w.drain()
		require.NoError(t, os.Rename(w.path("old"), w.path("new")))
		w.expect(t, dir(fsevents.Delete, "old"), dir(fsevents.Create, "new"), dir(fsevents.Create, "new/sub"))
		w.write(t, "new/sub/g.txt", "renamed")
		w.expect(t, file(fsevents.Create, "new/sub/g.txt"))
	})

	t.Run("Cancel", func(t *testing.T) {
		// Cancelling the context stops watching
		w := start(t)
		w.cancel()
		select {
		case err := <-w.done:
			assert.NoError(t, err)
		case <-time.After(timeout):
			t.Fatal("the watcher didn't stop")
		}
		w.done <- nil
	})

	t.Run("RootRemoved", func(t *testing.T) {
		// Removing the watched directory stops watching
		w := start(t)
		require.NoError(t, os.RemoveAll(w.dir))
		select {
		case err := <-w.done:
			assert.ErrorIs(t, err, fsevents.ErrRootRemoved)
		case <-time.After(timeout):
			t.Fatal("the watcher didn't stop")
		}
		w.done <- nil
	})

	t.Run("RootRenamed", func(t *testing.T) {
		// Renaming the watched directory stops watching
		w := start(t)
		moved := w.dir + "-moved"
		require.NoError(t, os.Rename(w.dir, moved))
		t.Cleanup(func() { os.RemoveAll(moved) })
		select {
		case err := <-w.done:
			assert.ErrorIs(t, err, fsevents.ErrRootRemoved)
		case <-time.After(timeout):
			t.Fatal("the watcher didn't stop")
		}
		w.done <- nil
	})

	t.Run("NotDirectory", func(t *testing.T) {
		name := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(name, nil, 0o600))
		err := fsevents.Watch(context.Background(), name, make(chan fsevents.Event))
		assert.Error(t, err)
		assert.False(t, errors.Is(err, fsevents.ErrNotSupported))
	})
}
