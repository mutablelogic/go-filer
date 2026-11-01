package filer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fsevents "github.com/mutablelogic/go-filer/filer/fs/fsevents"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// fileURL returns a file URL for a volume backed by a new directory in dir
func fileURL(t *testing.T, dir, volume string) *url.URL {
	t.Helper()
	path, err := os.MkdirTemp(dir, volume+"-")
	require.NoError(t, err)
	return &url.URL{Scheme: "file", Host: volume, Path: filepath.ToSlash(path)}
}

// names returns the names of the registered backends
func names(r *Registry) []string {
	r.RLock()
	defer r.RUnlock()
	var result []string
	for name := range r.backend {
		result = append(result, name)
	}
	return result
}

// get returns the registered backend for the volume, or nil
func get(r *Registry, volume string) gofiler.Filer {
	r.RLock()
	defer r.RUnlock()
	if e, ok := r.backend[volume]; ok {
		return e.Filer
	}
	return nil
}

// newRegistry returns a registry, and a directory for its volumes. The
// directory is created first, so that its cleanup runs after the registry is
// closed: a mounted volume keeps its directory open, and on Windows an open
// directory cannot be removed.
func newRegistry(t *testing.T) (*Registry, string) {
	t.Helper()
	dir := t.TempDir()
	r, err := New(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })
	return r, dir
}

func Test_Registry_Add(t *testing.T) {
	t.Run("Add", func(t *testing.T) {
		r, dir := newRegistry(t)
		b, err := r.Add(context.Background(), "media", fileURL(t, dir, "media"))
		require.NoError(t, err)
		assert.Equal(t, "media", b.URL().Host)
		assert.Same(t, b, get(r, "media"))
		assert.Equal(t, []string{"media"}, names(r))
	})

	t.Run("Duplicate", func(t *testing.T) {
		r, dir := newRegistry(t)
		first, err := r.Add(context.Background(), "media", fileURL(t, dir, "media"))
		require.NoError(t, err)

		// The existing backend is kept
		_, err = r.Add(context.Background(), "media", fileURL(t, dir, "media"))
		assert.ErrorIs(t, err, gofiler.ErrConflict)
		assert.Same(t, first, get(r, "media"))
	})

	t.Run("WrongVolume", func(t *testing.T) {
		r, dir := newRegistry(t)
		_, err := r.Add(context.Background(), "media", fileURL(t, dir, "other"))
		assert.ErrorIs(t, err, gofiler.ErrBadParameter)
		assert.Empty(t, names(r))
	})

	t.Run("MissingDirectory", func(t *testing.T) {
		r, dir := newRegistry(t)
		u := fileURL(t, dir, "media")
		u.Path += "/missing"
		_, err := r.Add(context.Background(), "media", u)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter)
		assert.Empty(t, names(r))
	})

	t.Run("UnsupportedScheme", func(t *testing.T) {
		r, _ := newRegistry(t)
		_, err := r.Add(context.Background(), "media", &url.URL{Scheme: "ftp", Host: "media", Path: "/data"})
		assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	})

	t.Run("NilURL", func(t *testing.T) {
		r, _ := newRegistry(t)
		_, err := r.Add(context.Background(), "media", nil)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	})
}

func Test_Registry_Del(t *testing.T) {
	t.Run("ClosesBackend", func(t *testing.T) {
		r, dir := newRegistry(t)
		b, err := r.Add(context.Background(), "media", fileURL(t, dir, "media"))
		require.NoError(t, err)

		require.NoError(t, r.Del(context.Background(), "media"))
		assert.Nil(t, get(r, "media"))
		assert.Empty(t, names(r))

		// The backend is closed
		_, err = b.GetObject(context.Background(), schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "/"},
		})
		assert.ErrorIs(t, err, fs.ErrClosed)
	})

	t.Run("Missing", func(t *testing.T) {
		r, _ := newRegistry(t)
		assert.NoError(t, r.Del(context.Background(), "missing"))
	})

	t.Run("AddAfterDel", func(t *testing.T) {
		r, dir := newRegistry(t)
		_, err := r.Add(context.Background(), "media", fileURL(t, dir, "media"))
		require.NoError(t, err)
		require.NoError(t, r.Del(context.Background(), "media"))
		_, err = r.Add(context.Background(), "media", fileURL(t, dir, "media"))
		assert.NoError(t, err)
	})
}

func Test_Registry_Close(t *testing.T) {
	dir := t.TempDir()
	r, err := New(context.Background())
	require.NoError(t, err)
	a, err := r.Add(context.Background(), "a", fileURL(t, dir, "a"))
	require.NoError(t, err)
	b, err := r.Add(context.Background(), "b", fileURL(t, dir, "b"))
	require.NoError(t, err)

	// Closing the registry closes every backend
	require.NoError(t, r.Close())
	for _, b := range []gofiler.Filer{a, b} {
		_, err := b.GetObject(context.Background(), schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "/"},
		})
		assert.ErrorIs(t, err, fs.ErrClosed, b.URL().Host)
	}
}

func Test_Registry_With(t *testing.T) {
	t.Run("CallsWithBackend", func(t *testing.T) {
		r, dir := newRegistry(t)
		added, err := r.Add(context.Background(), "media", fileURL(t, dir, "media"))
		require.NoError(t, err)

		var got gofiler.Filer
		require.NoError(t, r.With(context.Background(), "media", func(_ context.Context, b gofiler.Filer) error {
			got = b
			return nil
		}))
		assert.Same(t, added, got)
	})

	t.Run("NotMounted", func(t *testing.T) {
		r, _ := newRegistry(t)
		err := r.With(context.Background(), "missing", func(context.Context, gofiler.Filer) error {
			t.Error("fn called for a volume which is not mounted")
			return nil
		})
		assert.ErrorIs(t, err, gofiler.ErrNotFound)
	})

	t.Run("ReturnsError", func(t *testing.T) {
		r, dir := newRegistry(t)
		_, err := r.Add(context.Background(), "media", fileURL(t, dir, "media"))
		require.NoError(t, err)
		err = r.With(context.Background(), "media", func(context.Context, gofiler.Filer) error {
			return fs.ErrNotExist
		})
		assert.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("DelWaitsAndCancels", func(t *testing.T) {
		r, dir := newRegistry(t)
		_, err := r.Add(context.Background(), "media", fileURL(t, dir, "media"))
		require.NoError(t, err)

		// A call which runs until its context is cancelled, then still uses
		// the backend before returning
		entered := make(chan struct{})
		finished := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			result <- r.With(context.Background(), "media", func(ctx context.Context, b gofiler.Filer) error {
				close(entered)
				<-ctx.Done()
				_, err := b.GetObject(context.Background(), schema.GetObjectRequest{
					ObjectKey: schema.ObjectKey{Path: "/"},
				})
				close(finished)
				return errors.Join(ctx.Err(), err)
			})
		}()
		<-entered

		// Removing the volume cancels the call, and waits for it to finish
		// before closing the backend
		require.NoError(t, r.Del(context.Background(), "media"))
		select {
		case <-finished:
		default:
			t.Fatal("Del returned before the call using the backend finished")
		}

		// The call was cancelled, and the backend was still usable during it
		err = <-result
		assert.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, fs.ErrClosed, "backend was closed while in use")

		// New calls find the volume is not mounted
		err = r.With(context.Background(), "media", func(context.Context, gofiler.Filer) error {
			t.Error("fn called after the volume was removed")
			return nil
		})
		assert.ErrorIs(t, err, gofiler.ErrNotFound)
	})

	t.Run("CloseWaits", func(t *testing.T) {
		dir := t.TempDir()
		r, err := New(context.Background())
		require.NoError(t, err)
		_, err = r.Add(context.Background(), "media", fileURL(t, dir, "media"))
		require.NoError(t, err)

		entered := make(chan struct{})
		finished := make(chan struct{})
		go r.With(context.Background(), "media", func(ctx context.Context, b gofiler.Filer) error {
			close(entered)
			<-ctx.Done()
			close(finished)
			return nil
		})
		<-entered

		// Closing the registry stops the call and waits for it to finish
		require.NoError(t, r.Close())
		select {
		case <-finished:
		default:
			t.Fatal("Close returned before the call using the backend finished")
		}

		// Backends cannot be added after the registry is closed
		_, err = r.Add(context.Background(), "other", fileURL(t, dir, "other"))
		assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
	})
}

func Test_Registry_Validate(t *testing.T) {
	r, dir := newRegistry(t)

	// The URL which is validated has the backend's options, which aren't the
	// defaults, and adds the same backend
	u := fileURL(t, dir, "media")
	u.RawQuery = url.Values{"readonly": {"true"}, "package": {"(?i)\\.bundle$", "(?i)\\.sparsebundle$"}}.Encode()
	validated, err := r.Validate(context.Background(), u)
	require.NoError(t, err)
	assert.Equal(t, "file", validated.Scheme)
	assert.Equal(t, "media", validated.Host)
	assert.Equal(t, "true", validated.Query().Get("readonly"))
	assert.False(t, validated.Query().Has("package"), "the default packages are in the URL")

	b, err := r.Add(context.Background(), "media", validated)
	require.NoError(t, err)
	assert.Equal(t, validated.String(), b.URL().String())
	_, err = b.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.txt"}})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)

	// A URL which isn't valid isn't
	_, err = r.Validate(context.Background(), &url.URL{Scheme: "file", Host: "media", Path: u.Path, RawQuery: "unknown=1"})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}

func Test_Registry_AbsolutePath(t *testing.T) {
	r, _ := newRegistry(t)

	// A volume's path is absolute, so a URL without one isn't the directory
	// the registry is running in
	for _, u := range []*url.URL{
		{Scheme: "file", Host: "media"},
		{Scheme: "file", Host: "media", Path: "relative/dir"},
	} {
		_, err := r.Add(context.Background(), "media", u)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, u.String())
		_, err = r.Validate(context.Background(), u)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, u.String())
	}
	assert.Empty(t, names(r))
}

func Test_Registry_AddConcurrent(t *testing.T) {
	r, dir := newRegistry(t)

	// A volume which is being added can't be added again, while it's
	// created without the registry being locked
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			_, err := r.Add(context.Background(), "media", fileURL(t, dir, "media"))
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	added := 0
	for err := range errs {
		if err == nil {
			added++
		} else {
			assert.ErrorIs(t, err, gofiler.ErrConflict)
		}
	}
	assert.Equal(t, 1, added)
	assert.Equal(t, []string{"media"}, names(r))
}

func Test_Registry_DelWithin(t *testing.T) {
	r, dir := newRegistry(t)
	_, err := r.Add(context.Background(), "media", fileURL(t, dir, "media"))
	require.NoError(t, err)

	// Removing the volume from a function using it doesn't wait for the
	// function, which can still use the backend, and the backend is closed
	// once it returns
	var used gofiler.Filer
	done := make(chan error, 1)
	go func() {
		done <- r.With(context.Background(), "media", func(ctx context.Context, b gofiler.Filer) error {
			used = b
			if err := r.Del(ctx, "media"); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
			case <-time.After(10 * time.Second):
				return errors.New("the context wasn't cancelled")
			}
			_, err := b.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
			return err
		})
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("removing the volume from a function using it didn't return")
	}
	require.Eventually(t, func() bool { return get(r, "media") == nil }, 10*time.Second, 10*time.Millisecond)
	_, err = used.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	assert.ErrorIs(t, err, fs.ErrClosed)
}

func Test_Registry_DelCancelled(t *testing.T) {
	r, dir := newRegistry(t)
	_, err := r.Add(context.Background(), "media", fileURL(t, dir, "media"))
	require.NoError(t, err)

	// A function which doesn't return when it's cancelled holds up closing,
	// but not a Del whose context is cancelled
	release := make(chan struct{})
	entered := make(chan struct{})
	go r.With(context.Background(), "media", func(ctx context.Context, _ gofiler.Filer) error {
		close(entered)
		<-release
		return nil
	})
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, r.Del(ctx, "media"), context.DeadlineExceeded)

	// It's still being removed, and Close waits for it
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned before the volume which Del is removing was closed")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close didn't return")
	}
}

func Test_Registry_Keys(t *testing.T) {
	r, dir := newRegistry(t)
	assert.Empty(t, r.Keys())

	// The volumes which can be used are returned in order
	for _, volume := range []string{"music", "audiobooks", "podcasts"} {
		_, err := r.Add(context.Background(), volume, fileURL(t, dir, volume))
		require.NoError(t, err)
	}
	assert.Equal(t, []string{"audiobooks", "music", "podcasts"}, r.Keys())

	// A volume which is being removed isn't
	release, entered := make(chan struct{}), make(chan struct{})
	go r.With(context.Background(), "music", func(context.Context, gofiler.Filer) error {
		close(entered)
		<-release
		return nil
	})
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, r.Del(ctx, "music"), context.DeadlineExceeded)
	assert.Equal(t, []string{"audiobooks", "podcasts"}, r.Keys())
	close(release)
	require.NoError(t, r.Del(context.Background(), "music"))
	assert.Equal(t, []string{"audiobooks", "podcasts"}, r.Keys())

	// The keys can be changed without changing the registry
	keys := r.Keys()
	keys[0] = "changed"
	assert.Equal(t, []string{"audiobooks", "podcasts"}, r.Keys())

	// A closed registry has none
	require.NoError(t, r.Close())
	assert.Empty(t, r.Keys())
}

func Test_Registry_Schemes(t *testing.T) {
	r, dir := newRegistry(t)
	assert.Equal(t, []string{"file"}, r.Schemes())

	// Each scheme can be validated, and an unsupported one can't
	u := fileURL(t, dir, "media")
	for _, scheme := range r.Schemes() {
		u.Scheme = scheme
		_, err := r.Validate(context.Background(), u)
		assert.NoError(t, err, scheme)
	}
	u.Scheme = "ftp"
	_, err := r.Validate(context.Background(), u)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)

	// The schemes can be changed without changing the registry
	r.Schemes()[0] = "changed"
	assert.Equal(t, []string{"file"}, r.Schemes())
}

// watchAll watches every volume of the registry, until the test ends, and
// returns the changes
func watchAll(t *testing.T, r *Registry, size int) chan schema.Event {
	t.Helper()
	events, done := make(chan schema.Event, size), make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- r.WatchObjects(ctx, events) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			assert.True(t, err == nil || errors.Is(err, fs.ErrClosed), "watching: %v", err)
		case <-time.After(10 * time.Second):
			t.Error("watching didn't stop")
		}
	})
	time.Sleep(50 * time.Millisecond)
	return events
}

// waitFor writes a file to the directory until a change which matches is
// received, or fails the test
func waitFor(t *testing.T, events chan schema.Event, dir string, match func(schema.Event) bool) {
	t.Helper()
	for deadline := time.After(10 * time.Second); ; {
		os.WriteFile(filepath.Join(dir, "probe.txt"), nil, 0o600)
		select {
		case event := <-events:
			if match(event) {
				return
			}
		case <-time.After(100 * time.Millisecond):
		case <-deadline:
			t.Fatal("no change which matches")
		}
	}
}

func Test_Registry_WatchObjects(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	r, dir := newRegistry(t)
	events := watchAll(t, r, 1024)

	// The changes to each volume are sent, with its name, including those
	// added while watching
	a, b := fileURL(t, dir, "a"), fileURL(t, dir, "b")
	for volume, u := range map[string]*url.URL{"a": a, "b": b} {
		_, err := r.Add(context.Background(), volume, u)
		require.NoError(t, err)
	}
	for volume, u := range map[string]*url.URL{"a": a, "b": b} {
		waitFor(t, events, filepath.FromSlash(u.Path), func(event schema.Event) bool {
			return event.Name == volume && event.Path == "/probe.txt"
		})
	}

	// A volume which is removed isn't watched
	require.NoError(t, r.Del(context.Background(), "a"))
	time.Sleep(200 * time.Millisecond)
	for len(events) > 0 {
		<-events
	}
	require.NoError(t, os.WriteFile(filepath.Join(filepath.FromSlash(a.Path), "b.txt"), nil, 0o600))
	select {
	case event := <-events:
		assert.NotEqual(t, "a", event.Name, "change once the volume was removed: %v", event)
	case <-time.After(500 * time.Millisecond):
	}

	// Closing the registry stops watching
	done := make(chan error, 1)
	go func() { done <- r.WatchObjects(context.Background(), make(chan schema.Event)) }()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, r.Close())
	select {
	case err := <-done:
		assert.ErrorIs(t, err, fs.ErrClosed)
	case <-time.After(10 * time.Second):
		t.Fatal("watching didn't stop when the registry was closed")
	}
	assert.ErrorIs(t, r.WatchObjects(context.Background(), make(chan schema.Event)), fs.ErrClosed)
}

func Test_Registry_WatchSlow(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	queue := watchQueue
	watchQueue = 2
	t.Cleanup(func() { watchQueue = queue })

	// A caller which doesn't read the changes doesn't hold up removing a
	// volume, or the other callers, and is sent a rescan in place of those
	// which were dropped
	r, dir := newRegistry(t)
	slow := make(chan schema.Event)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.WatchObjects(ctx, slow)
	fast := watchAll(t, r, 1024)
	u := fileURL(t, dir, "slow")
	_, err := r.Add(context.Background(), "slow", u)
	require.NoError(t, err)
	path := filepath.FromSlash(u.Path)
	waitFor(t, fast, path, func(event schema.Event) bool { return event.Name == "slow" })
	for i := range 50 {
		require.NoError(t, os.WriteFile(filepath.Join(path, fmt.Sprintf("%d.txt", i)), nil, 0o600))
	}
	time.Sleep(500 * time.Millisecond)

	// The slow caller gets some changes, then a rescan
	rescan := false
	for deadline := time.After(10 * time.Second); !rescan; {
		select {
		case event := <-slow:
			rescan = event.EventType == schema.EventTypeRescan && event.Name == "slow" && event.Path == "/"
		case <-deadline:
			t.Fatal("no rescan once changes were dropped")
		}
	}

	// And removing the volume isn't held up
	removed := make(chan error, 1)
	go func() { removed <- r.Del(context.Background(), "slow") }()
	select {
	case err := <-removed:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("removing the volume waited for a caller which doesn't read changes")
	}
}

func Test_Registry_WatchRestart(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	} else if runtime.GOOS == "windows" {
		t.Skip("a directory which is open can't be removed on Windows")
	}
	retry := watchRetry
	watchRetry = 10 * time.Millisecond
	t.Cleanup(func() { watchRetry = retry })

	// A volume whose watching fails is watched again, with a rescan, once
	// it can be
	r, dir := newRegistry(t)
	events := watchAll(t, r, 1024)
	u := fileURL(t, dir, "restart")
	_, err := r.Add(context.Background(), "restart", u)
	require.NoError(t, err)
	path := filepath.FromSlash(u.Path)
	waitFor(t, events, path, func(event schema.Event) bool { return event.Name == "restart" })

	// Remove the directory, which stops watching it, and put it back
	require.NoError(t, os.RemoveAll(path))
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, os.Mkdir(path, 0o755))
	waitFor(t, events, path, func(event schema.Event) bool {
		return event.EventType == schema.EventTypeRescan && event.Name == "restart" && event.Path == "/"
	})

	// The volume is opened again, so it's of the new directory, whose
	// objects are got, and whose changes are sent
	require.NoError(t, os.WriteFile(filepath.Join(path, "new.txt"), []byte("new"), 0o600))
	obj, err := r.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Name: "restart", Path: "/new.txt"}})
	require.NoError(t, err)
	assert.Equal(t, int64(3), *obj.Size)
	waitFor(t, events, path, func(event schema.Event) bool {
		return event.Name == "restart" && event.Path == "/probe.txt"
	})
}

func Test_Registry_Objects(t *testing.T) {
	r, dir := newRegistry(t)
	ctx := context.Background()
	for _, volume := range []string{"a", "b"} {
		_, err := r.Add(ctx, volume, fileURL(t, dir, volume))
		require.NoError(t, err)
	}
	key := func(name, path string) schema.ObjectKey {
		return schema.ObjectKey{Name: name, Path: path}
	}

	// Each request is for the volume it names, and the objects returned have
	// the name of their volume
	for _, volume := range []string{"a", "b"} {
		obj, err := r.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: key(volume, "/"+volume+".txt"), Body: strings.NewReader(volume)})
		require.NoError(t, err)
		assert.Equal(t, key(volume, "/"+volume+".txt"), obj.ObjectKey)
	}
	obj, err := r.GetObject(ctx, schema.GetObjectRequest{ObjectKey: key("a", "/a.txt")})
	require.NoError(t, err)
	assert.Equal(t, "a", obj.Name)
	_, err = r.GetObject(ctx, schema.GetObjectRequest{ObjectKey: key("a", "/b.txt")})
	assert.ErrorIs(t, err, fs.ErrNotExist)

	read, obj, err := r.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: key("b", "/b.txt")})
	require.NoError(t, err)
	assert.Equal(t, "b", obj.Name)
	var buf strings.Builder
	require.NoError(t, read(ctx, func(r io.Reader) error { _, err := io.Copy(&buf, r); return err }))
	assert.Equal(t, "b", buf.String())

	iter, err := r.ListObjects(ctx, schema.ListObjectsRequest{ObjectKey: key("a", "/")})
	require.NoError(t, err)
	obj, err = iter.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, key("a", "/a.txt"), obj.ObjectKey)
	_, err = iter.Next(ctx)
	assert.ErrorIs(t, err, io.EOF)
	require.NoError(t, iter.Close())
	require.NoError(t, iter.Close())
	_, err = iter.Next(ctx)
	assert.ErrorIs(t, err, fs.ErrClosed)

	deleted, err := r.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: key("a", "/a.txt")})
	require.NoError(t, err)
	require.Len(t, deleted.Body, 1)
	assert.Equal(t, key("a", "/a.txt"), deleted.Body[0].ObjectKey)

	// A volume which isn't mounted isn't found, by every method
	missing := key("missing", "/a.txt")
	_, err = r.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: missing})
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, err = r.GetObject(ctx, schema.GetObjectRequest{ObjectKey: missing})
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, _, err = r.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: missing})
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, err = r.ListObjects(ctx, schema.ListObjectsRequest{ObjectKey: missing})
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, err = r.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: missing})
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	assert.Equal(t, "filer:", r.URL().String())
}

func Test_Registry_ObjectsRemoved(t *testing.T) {
	r, dir := newRegistry(t)
	ctx := context.Background()
	u := fileURL(t, dir, "media")
	_, err := r.Add(ctx, "media", u)
	require.NoError(t, err)
	_, err = r.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Name: "media", Path: "/a.txt"}, Body: strings.NewReader("a")})
	require.NoError(t, err)

	// An iterator which isn't closed doesn't stop the volume being removed,
	// which closes it, and a read of a volume which was removed, and added
	// again, isn't of the new one
	iter, err := r.ListObjects(ctx, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Name: "media", Path: "/"}})
	require.NoError(t, err)
	read, _, err := r.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Name: "media", Path: "/a.txt"}})
	require.NoError(t, err)

	removed := make(chan error, 1)
	go func() { removed <- r.Del(ctx, "media") }()
	select {
	case err := <-removed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("removing the volume waited for an iterator which wasn't closed")
	}
	_, err = iter.Next(ctx)
	assert.ErrorIs(t, err, fs.ErrClosed)
	assert.NoError(t, iter.Close())

	_, err = r.Add(ctx, "media", u)
	require.NoError(t, err)
	assert.ErrorIs(t, read(ctx, func(io.Reader) error { return nil }), gofiler.ErrNotFound)
}

func Test_Registry_Volume(t *testing.T) {
	r, dir := newRegistry(t)
	u := fileURL(t, dir, "media")
	b, err := r.Add(context.Background(), "media", u)
	require.NoError(t, err)

	// A volume which is registered has the URL of its backend, and one which
	// isn't, or is being removed, isn't found
	volume, err := r.Volume("media")
	require.NoError(t, err)
	assert.Equal(t, schema.Volume{Name: "media", URL: b.URL().String()}, *volume)
	_, err = r.Volume("missing")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	require.NoError(t, r.Del(context.Background(), "media"))
	_, err = r.Volume("media")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
}

func Test_Registry_Update(t *testing.T) {
	r, dir := newRegistry(t)
	ctx := context.Background()
	u := fileURL(t, dir, "media")
	u.RawQuery = url.Values{"skip": {`\.tmp$`}}.Encode()
	_, err := r.Add(ctx, "media", u)
	require.NoError(t, err)
	query := func() url.Values {
		volume, err := r.Volume("media")
		require.NoError(t, err)
		u, err := url.Parse(volume.URL)
		require.NoError(t, err)
		return u.Query()
	}
	create := func(path string) error {
		_, err := r.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Name: "media", Path: path}})
		return err
	}

	// An option is added, and the others are kept, and the volume is opened
	// again with them
	volume, err := r.Update(ctx, "media", url.Values{"readonly": {"true"}})
	require.NoError(t, err)
	assert.Equal(t, "media", volume.Name)
	assert.Equal(t, url.Values{"readonly": {"true"}, "skip": {`\.tmp$`}}, query())
	assert.ErrorIs(t, create("/a.txt"), gofiler.ErrForbidden)

	// An option is replaced, and one without values is removed, so it's the
	// default
	_, err = r.Update(ctx, "media", url.Values{"readonly": {"false"}, "skip": {}})
	require.NoError(t, err)
	assert.Empty(t, query())
	assert.NoError(t, create("/a.txt"))

	// Options which are invalid don't change the volume
	for _, patch := range []url.Values{{"unknown": {"1"}}, {"readonly": {"maybe"}}, {"skip": {"("}}} {
		_, err := r.Update(ctx, "media", patch)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, patch)
		assert.Empty(t, query(), patch)
	}

	// A volume which isn't registered isn't found
	_, err = r.Update(ctx, "missing", url.Values{"readonly": {"true"}})
	assert.ErrorIs(t, err, gofiler.ErrNotFound)

	// A volume can't be updated from a function using it, which it'd wait for
	require.NoError(t, r.With(ctx, "media", func(ctx context.Context, _ gofiler.Filer) error {
		_, err := r.Update(ctx, "media", url.Values{"readonly": {"true"}})
		assert.ErrorIs(t, err, gofiler.ErrConflict)
		return nil
	}))

	// Updating waits for the calls using the volume, which are cancelled,
	// and the volume can't be added meanwhile
	entered, release := make(chan struct{}), make(chan struct{})
	go r.With(ctx, "media", func(ctx context.Context, _ gofiler.Filer) error {
		close(entered)
		<-ctx.Done()
		<-release
		return nil
	})
	<-entered
	updated := make(chan error, 1)
	go func() {
		_, err := r.Update(ctx, "media", url.Values{"readonly": {"true"}})
		updated <- err
	}()
	require.Eventually(t, func() bool {
		_, err := r.Add(ctx, "media", fileURL(t, dir, "media"))
		return errors.Is(err, gofiler.ErrConflict) && !slices.Contains(r.Keys(), "media")
	}, 10*time.Second, 10*time.Millisecond)
	select {
	case err := <-updated:
		t.Fatalf("updated before the call using the volume returned: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-updated)
	assert.Equal(t, url.Values{"readonly": {"true"}}, query())
}

func Test_Registry_UpdateMissing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("a directory which is open can't be removed on Windows")
	}
	r, dir := newRegistry(t)
	ctx := context.Background()
	u := fileURL(t, dir, "media")
	_, err := r.Add(ctx, "media", u)
	require.NoError(t, err)

	// When the volume's directory is removed, its options can't be checked,
	// as a backend can't be created for them, so the volume isn't closed
	require.NoError(t, os.RemoveAll(filepath.FromSlash(u.Path)))
	_, err = r.Update(ctx, "media", url.Values{"readonly": {"true"}})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = r.Volume("media")
	assert.NoError(t, err, "the volume was removed when it couldn't be updated")
}

func Test_Registry_CloseWaitsForUpdate(t *testing.T) {
	r, dir := newRegistry(t)
	ctx := context.Background()
	_, err := r.Add(ctx, "media", fileURL(t, dir, "media"))
	require.NoError(t, err)

	// Close waits for the backend which Update is closing, which is no longer
	// the volume's in the registry, as Update waits for a call using it
	entered, release := make(chan struct{}), make(chan struct{})
	go r.With(ctx, "media", func(ctx context.Context, _ gofiler.Filer) error {
		close(entered)
		<-ctx.Done()
		<-release
		return nil
	})
	<-entered
	go r.Update(ctx, "media", url.Values{"readonly": {"true"}})
	require.Eventually(t, func() bool { return !slices.Contains(r.Keys(), "media") }, 10*time.Second, 10*time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned before the backend which Update is closing was closed")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close didn't return")
	}
}

func Test_Registry_Cancelled(t *testing.T) {
	r, dir := newRegistry(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	// A volume isn't added, or updated, with a context which is cancelled
	_, err := r.Add(cancelled, "media", fileURL(t, dir, "media"))
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, r.Keys())
	_, err = r.Add(context.Background(), "media", fileURL(t, dir, "media"))
	require.NoError(t, err)
	_, err = r.Update(cancelled, "media", url.Values{"readonly": {"true"}})
	assert.ErrorIs(t, err, context.Canceled)
	volume, err := r.Volume("media")
	require.NoError(t, err)
	assert.NotContains(t, volume.URL, "readonly")
}

func Test_Registry_InvalidQuery(t *testing.T) {
	r, dir := newRegistry(t)

	// Options which can't be parsed are rejected, rather than left out, so
	// readonly=%ZZ doesn't mount a volume which can be written
	u := fileURL(t, dir, "media")
	u.RawQuery = "readonly=%ZZ"
	_, err := r.Validate(context.Background(), u)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = r.Add(context.Background(), "media", u)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	assert.Empty(t, r.Keys())
}

func Test_Registry_UpdateRescan(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}

	// A volume which is updated is rescanned, as its changes are missed
	// while it's closed, and its options can change which objects it has
	r, dir := newRegistry(t)
	events := watchAll(t, r, 1024)
	_, err := r.Add(context.Background(), "media", fileURL(t, dir, "media"))
	require.NoError(t, err)
	_, err = r.Update(context.Background(), "media", url.Values{"skip": {""}})
	require.NoError(t, err)
	for deadline := time.After(10 * time.Second); ; {
		select {
		case event := <-events:
			if event.EventType == schema.EventTypeRescan && event.Name == "media" && event.Path == "/" {
				return
			}
		case <-deadline:
			t.Fatal("no rescan once the volume was updated")
		}
	}
}
