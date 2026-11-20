package registry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fsevents "github.com/mutablelogic/go-filer/backend/fs/fsevents"
	indexer "github.com/mutablelogic/go-filer/indexer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newIndexedRegistry returns a running registry, with an index store, and
// a directory for its volumes
func newIndexedRegistry(t *testing.T) (*Registry, *sqlite.Store, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := sqlite.New(context.Background(), gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	r, err := New(context.Background(), WithDebounce(0), WithBroadcaster(newBroadcaster(t)), WithStore(s))
	require.NoError(t, err)
	t.Cleanup(func() { r.close() })
	runRegistry(t, r)
	return r, s, dir
}

// indexedURL returns a file URL of a new directory, with a folder, and an
// index option, unless it's empty
func indexedURL(t *testing.T, dir, volume, index string) *url.URL {
	t.Helper()
	u := fileURL(t, dir, volume)
	require.NoError(t, os.MkdirAll(filepath.Join(filepath.FromSlash(u.Path), "a", "b"), 0o755))
	if index != "" {
		u.RawQuery = url.Values{indexer.IndexParam: {index}}.Encode()
	}
	return u
}

// ready waits for a volume's index to be ready
func ready(t *testing.T, r *Registry, volume string) *schema.Volume {
	t.Helper()
	var v *schema.Volume
	require.Eventually(t, func() bool {
		var err error
		v, err = r.GetVolume(context.Background(), volume)
		return err == nil && v.Index != nil && v.Index.Status == schema.IndexStatusReady
	}, 10*time.Second, 5*time.Millisecond, "the index of %q isn't ready", volume)
	return v
}

// indexedFolders lists the folders of a volume's index, beneath the root
func indexedFolders(t *testing.T, r *Registry, volume string) []string {
	t.Helper()
	i, ok := get(r, volume).(*indexer.Indexer)
	require.True(t, ok, "volume %q isn't indexed", volume)
	iter, err := i.ListFolders(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true})
	require.NoError(t, err)
	defer iter.Close()
	result := []string{}
	for {
		object, err := iter.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return result
		}
		require.NoError(t, err)
		result = append(result, object.Path)
	}
}

func Test_Registry_IndexOption(t *testing.T) {
	// A registry needs a store
	_, err := New(context.Background())
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)

	// An option which isn't a mode is bad, and a volume without it isn't
	// indexed, which its URL says
	r, _, dir := newIndexedRegistry(t)
	_, err = add(t, r, "bad", indexedURL(t, dir, "bad", "everything"))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = add(t, r, "plain", indexedURL(t, dir, "plain", ""))
	require.NoError(t, err)
	v, err := r.GetVolume(context.Background(), "plain")
	require.NoError(t, err)
	u, err := url.Parse(v.URL)
	require.NoError(t, err)
	assert.Equal(t, "none", u.Query().Get(indexer.IndexParam))
	assert.Nil(t, v.Index)
	_, indexed := get(r, "plain").(*indexer.Indexer)
	assert.False(t, indexed)
}

func Test_Registry_Indexed(t *testing.T) {
	r, s, dir := newIndexedRegistry(t)

	// A volume with index=folders is indexed, by the registry, and its URL,
	// and index, say so
	_, err := add(t, r, "media", indexedURL(t, dir, "media", "folders"))
	require.NoError(t, err)
	v := ready(t, r, "media")
	assert.Equal(t, "folders", v.Index.Mode)
	assert.NotZero(t, v.Index.Folders)
	u, err := url.Parse(v.URL)
	require.NoError(t, err)
	assert.Equal(t, "folders", u.Query().Get(indexer.IndexParam))
	assert.Equal(t, []string{"/a", "/a/b"}, indexedFolders(t, r, "media"))

	// It's turned off, which removes its index, and on, again, by updating
	// it
	v, err = update(t, r, "media", url.Values{indexer.IndexParam: {"none"}})
	require.NoError(t, err)
	assert.Nil(t, v.Index)
	_, indexed := get(r, "media").(*indexer.Indexer)
	assert.False(t, indexed)
	state, err := s.State(context.Background(), "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, state.Status, "its index is removed")
	_, err = update(t, r, "media", url.Values{indexer.IndexParam: {"folders"}})
	require.NoError(t, err)
	ready(t, r, "media")

	// An update of its other options keeps it indexed
	_, err = update(t, r, "media", url.Values{"readonly": {"true"}})
	require.NoError(t, err)
	v = ready(t, r, "media")
	readonly, err := v.Readonly()
	require.NoError(t, err)
	assert.True(t, readonly)
	_, err = update(t, r, "media", url.Values{indexer.IndexParam: {"everything"}})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)

	// Deleting it removes its index
	require.NoError(t, del(t, r, "media"))
	state, err = s.State(context.Background(), "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, state.Status)
	_, err = s.GetObject(context.Background(), "media", "/")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
}

func Test_Registry_IndexedWatch(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}

	// The changes of an indexed volume are sent, of the indexer, which is
	// the only watch of the backend, and its index follows them
	r, _, dir := newIndexedRegistry(t)
	events := watchAll(t, r, 1024)
	u := indexedURL(t, dir, "media", "folders")
	_, err := add(t, r, "media", u)
	require.NoError(t, err)
	ready(t, r, "media")
	path := filepath.FromSlash(u.Path)
	waitFor(t, events, path, func(event schema.Event) bool {
		return event.Name == "media" && event.EventType != schema.EventTypeRescan
	})
	require.NoError(t, os.MkdirAll(filepath.Join(path, "c"), 0o755))
	require.Eventually(t, func() bool {
		folders := indexedFolders(t, r, "media")
		return len(folders) == 3 && folders[2] == "/c"
	}, 10*time.Second, 10*time.Millisecond, "the index has /c")
}

func Test_Registry_ListType(t *testing.T) {
	r, _, dir := newIndexedRegistry(t)
	list := func(volume, typ string) []string {
		t.Helper()
		iter, err := r.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Name: volume, Path: "/"}, Recursive: true, Type: typ})
		require.NoError(t, err)
		defer iter.Close()
		result := []string{}
		for {
			object, err := iter.Next(context.Background())
			if errors.Is(err, io.EOF) {
				return result
			}
			require.NoError(t, err)
			result = append(result, object.Path)
		}
	}

	// A listing of a type, of a volume which isn't indexed, is filtered, of
	// a media type, or of its major type
	u := indexedURL(t, dir, "plain", "")
	require.NoError(t, os.WriteFile(filepath.Join(filepath.FromSlash(u.Path), "a", "photo.png"), []byte("png"), 0o644))
	_, err := add(t, r, "plain", u)
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/a/b"}, list("plain", "inode/directory"))
	assert.Equal(t, []string{"/a", "/a/b"}, list("plain", "inode"))
	assert.Equal(t, []string{"/a/photo.png"}, list("plain", "image"))
	assert.Equal(t, []string{"/a", "/a/b", "/a/photo.png"}, list("plain", ""))

	// Of folders, of an indexed volume, it's its index's, once it's ready,
	// which has a folder the backend doesn't, until it's scanned again, which
	// the watch's first rescan can do, so it's put until it stays
	_, err = add(t, r, "media", indexedURL(t, dir, "media", "folders"))
	require.NoError(t, err)
	ready(t, r, "media")
	s := r.store
	require.Eventually(t, func() bool {
		require.NoError(t, s.PutObject(context.Background(), "media", &schema.Object{ObjectKey: schema.ObjectKey{Path: "/only-in-the-index"}, ObjectAttr: schema.ObjectAttr{IsDir: true}}))
		time.Sleep(50 * time.Millisecond)
		return len(list("media", "inode")) == 3
	}, 10*time.Second, time.Millisecond, "the index keeps a folder the backend doesn't")
	for _, typ := range []string{"inode", "directory", "inode/directory", "inode/directory; charset=binary"} {
		assert.Equal(t, []string{"/a", "/a/b", "/only-in-the-index"}, list("media", typ), typ)
	}
	assert.Equal(t, []string{"/a", "/a/b"}, list("media", ""), "every object is the backend's")
}

func Test_Registry_DeleteIndex(t *testing.T) {
	r, s, dir := newIndexedRegistry(t)
	_, err := add(t, r, "media", indexedURL(t, dir, "media", "folders"))
	require.NoError(t, err)
	ready(t, r, "media")

	// Deleting the volume removes its index, with it, from the store, and
	// it's unmounted
	_, err = r.DeleteVolume(context.Background(), "media")
	require.NoError(t, err)
	state, err := s.State(context.Background(), "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, state.Status)
	_, err = s.GetObject(context.Background(), "media", "/")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	require.Eventually(t, func() bool { return get(r, "media") == nil }, mountTimeout, 5*time.Millisecond)
}

// failingStore is a store whose objects can't be put
type failingStore struct {
	store.Store
}

func (failingStore) PutObject(context.Context, string, ...*schema.Object) error {
	return gofiler.ErrNotImplemented.With("objects can't be put")
}

func Test_Registry_IndexStoreError(t *testing.T) {
	// A store which can't index, as its objects can't be put, is why the
	// volume isn't indexed, in its index, while the volume is stored
	s, err := sqlite.New(context.Background(), gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	dir := t.TempDir()
	r, err := New(context.Background(), WithDebounce(0), WithBroadcaster(newBroadcaster(t)), WithStore(failingStore{s}))
	require.NoError(t, err)
	t.Cleanup(func() { r.close() })
	runRegistry(t, r)
	_, err = add(t, r, "media", indexedURL(t, dir, "media", "folders"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		v, err := r.GetVolume(context.Background(), "media")
		return err == nil && v.Index != nil && strings.Contains(v.Index.Error, "can't be put")
	}, 10*time.Second, 5*time.Millisecond, "the index has the store's error")
	v, err := r.GetVolume(context.Background(), "media")
	require.NoError(t, err)
	assert.Equal(t, "folders", v.Index.Mode)
	volume, err := s.GetVolume(context.Background(), "media")
	require.NoError(t, err)
	assert.Contains(t, volume.URL, "index=folders")
}

func Test_Registry_StoredVolumes(t *testing.T) {
	r, s, dir := newIndexedRegistry(t)
	ctx := context.Background()

	// A volume which is added is stored, enabled, with its URL, as the
	// registry has it, with its index option, whether it's indexed or not
	_, err := add(t, r, "plain", indexedURL(t, dir, "plain", ""))
	require.NoError(t, err)
	_, err = add(t, r, "media", indexedURL(t, dir, "media", "folders"))
	require.NoError(t, err)
	volumes, err := s.ListVolumes(ctx)
	require.NoError(t, err)
	require.Len(t, volumes, 2)
	assert.Equal(t, "media", volumes[0].Name)
	assert.Contains(t, volumes[0].URL, "index=folders")
	assert.Contains(t, volumes[1].URL, "index=none")
	for _, volume := range volumes {
		assert.True(t, volume.Enabled)
		registered, err := r.GetVolume(context.Background(), volume.Name)
		require.NoError(t, err)
		assert.Equal(t, registered.URL, volume.URL)
	}

	// An update stores its options
	_, err = update(t, r, "media", url.Values{"readonly": {"true"}, indexer.IndexParam: {"none"}})
	require.NoError(t, err)
	volume, err := s.GetVolume(ctx, "media")
	require.NoError(t, err)
	assert.Contains(t, volume.URL, "index=none")
	assert.Contains(t, volume.URL, "readonly=true")
	assert.False(t, volume.Modified.IsZero())

	// Deleting it removes it from the store
	require.NoError(t, del(t, r, "media"))
	_, err = s.GetVolume(ctx, "media")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
}

func Test_Registry_RecreateVolume(t *testing.T) {
	r, dir := newRegistry(t)
	u := fileURL(t, dir, "media")
	_, err := add(t, r, "media", u)
	require.NoError(t, err)
	r.RLock()
	old := r.backend["media"]
	r.RUnlock()

	// A volume which is deleted, and created again, of the same URL, before
	// it's synced, such as by another process, whose changes are one sync, is
	// mounted again, as it's another stored volume
	stored, err := r.GetVolume(context.Background(), "media")
	require.NoError(t, err)
	require.NoError(t, r.store.DeleteVolume(context.Background(), "media"))
	time.Sleep(time.Millisecond)
	_, err = r.store.CreateVolume(context.Background(), stored.VolumeMeta)
	require.NoError(t, err)
	r.sync("media", false)
	_, err = mounted(t, r, "media", old)
	require.NoError(t, err)
	select {
	case <-old.closed:
	case <-time.After(mountTimeout):
		t.Fatal("the old backend isn't closed")
	}
}

func Test_Registry_AnotherBackend(t *testing.T) {
	r, s, dir := newIndexedRegistry(t)
	ctx := context.Background()
	_, err := add(t, r, "media", indexedURL(t, dir, "media", "folders"))
	require.NoError(t, err)
	ready(t, r, "media")
	require.Equal(t, []string{"/a", "/a/b"}, indexedFolders(t, r, "media"))

	// A volume whose backend is another, of its path, doesn't keep the old
	// backend's index, which is scanned again
	other := fileURL(t, dir, "media")
	require.NoError(t, os.MkdirAll(filepath.Join(filepath.FromSlash(other.Path), "c"), 0o755))
	other.RawQuery = url.Values{indexer.IndexParam: {"folders"}}.Encode()
	r.RLock()
	old := r.backend["media"]
	r.RUnlock()
	_, err = r.UpdateVolume(ctx, schema.VolumeMeta{Name: "media", URL: other.String(), Enabled: true})
	require.NoError(t, err)
	_, err = mounted(t, r, "media", old)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		v, err := r.GetVolume(ctx, "media")
		return err == nil && v.Index != nil && v.Index.Status == schema.IndexStatusReady && slices.Equal([]string{"/c"}, indexedFolders(t, r, "media"))
	}, mountTimeout, 5*time.Millisecond, "the index is of the new backend")

	// Nor does one which isn't mounted, when its backend is another
	disabled := schema.VolumeMeta{Name: "media", URL: other.String(), Enabled: false}
	_, err = r.UpdateVolume(ctx, disabled)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return get(r, "media") == nil }, mountTimeout, 5*time.Millisecond)
	third := indexedURL(t, dir, "media", "folders")
	disabled.URL = third.String()
	_, err = r.UpdateVolume(ctx, disabled)
	require.NoError(t, err)
	_, err = s.GetObject(ctx, "media", "/c/")
	assert.ErrorIs(t, err, gofiler.ErrNotFound, "the old backend's index is removed")
}

func Test_Registry_DisabledIndex(t *testing.T) {
	r, s, dir := newIndexedRegistry(t)
	ctx := context.Background()
	u := indexedURL(t, dir, "media", "folders")
	_, err := add(t, r, "media", u)
	require.NoError(t, err)
	ready(t, r, "media")

	// A volume which is disabled, and isn't indexed any more, doesn't keep
	// its index, once it's unmounted
	u.RawQuery = url.Values{indexer.IndexParam: {"none"}}.Encode()
	_, err = r.UpdateVolume(ctx, schema.VolumeMeta{Name: "media", URL: u.String(), Enabled: false})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		state, err := s.State(ctx, "media")
		if err != nil || state.Status != schema.IndexStatusNone {
			return false
		}
		_, err = s.GetObject(ctx, "media", "/a/")
		return errors.Is(err, gofiler.ErrNotFound) && get(r, "media") == nil
	}, mountTimeout, 5*time.Millisecond, "its index is removed")
}

// flakyStore is a store whose volumes can't be listed, the first times
type flakyStore struct {
	store.Store
	fails atomic.Int32
}

func (s *flakyStore) ListVolumes(ctx context.Context) ([]*schema.Volume, error) {
	if s.fails.Add(-1) >= 0 {
		return nil, gofiler.ErrServiceUnavailable.With("the store is down")
	}
	return s.Store.ListVolumes(ctx)
}

func Test_Registry_ListRetry(t *testing.T) {
	retry := watchRetry
	watchRetry = 10 * time.Millisecond
	t.Cleanup(func() { watchRetry = retry })

	// A registry whose store can't be listed, when it runs, lists it again,
	// after a while, until it can be, when it mounts its volumes
	dir := t.TempDir()
	s := &flakyStore{Store: newStore(t)}
	r, err := New(context.Background(), WithDebounce(0), WithStore(s))
	require.NoError(t, err)
	t.Cleanup(func() { r.close() })
	u, err := validate(r, fileURL(t, dir, "media"))
	require.NoError(t, err)
	_, err = s.Store.CreateVolume(context.Background(), schema.VolumeMeta{Name: "media", URL: u.String(), Enabled: true})
	require.NoError(t, err)
	s.fails.Store(3)
	runRegistry(t, r)
	_, err = mounted(t, r, "media", nil)
	require.NoError(t, err)
	assert.Less(t, s.fails.Load(), int32(0), "it's listed again")
}

func Test_Registry_UpdateScheme(t *testing.T) {
	r, dir := newRegistry(t)
	ctx := context.Background()
	u := fileURL(t, dir, "media")
	_, err := add(t, r, "media", u)
	require.NoError(t, err)

	// A volume's scheme isn't changed, and it's kept as it is
	stored, err := r.GetVolume(ctx, "media")
	require.NoError(t, err)
	_, err = r.UpdateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "s3://media/?endpoint=http://localhost:1", Enabled: true})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	again, err := r.GetVolume(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, stored.URL, again.URL)
}

func Test_Registry_IndexObjects(t *testing.T) {
	r, s, dir := newIndexedRegistry(t)
	ctx := context.Background()
	u := indexedURL(t, dir, "media", "objects")
	require.NoError(t, os.WriteFile(filepath.Join(filepath.FromSlash(u.Path), "a", "photo.png"), []byte("png"), 0o644))

	// A volume with index=objects has its files indexed, which its URL, and
	// index, say
	_, err := add(t, r, "media", u)
	require.NoError(t, err)
	v := ready(t, r, "media")
	assert.Equal(t, "objects", v.Index.Mode)
	stored, err := url.Parse(v.URL)
	require.NoError(t, err)
	assert.Equal(t, "objects", stored.Query().Get(indexer.IndexParam))
	require.Eventually(t, func() bool {
		_, err := s.GetObject(ctx, "media", "/a/photo.png")
		return err == nil
	}, mountTimeout, 5*time.Millisecond, "its file is indexed")

	// Indexed of its folders only, it doesn't keep its files, once it's
	// scanned
	v, err = update(t, r, "media", url.Values{indexer.IndexParam: {"folders"}})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		v, err := r.GetVolume(ctx, "media")
		if err != nil || v.Index == nil || v.Index.Mode != "folders" {
			return false
		}
		_, err = s.GetObject(ctx, "media", "/a/photo.png")
		return errors.Is(err, gofiler.ErrNotFound)
	}, mountTimeout, 5*time.Millisecond, "its file isn't indexed")
	assert.Equal(t, []string{"/a", "/a/b"}, indexedFolders(t, r, "media"))

	// And raised to objects again, its folders are kept, and its files are
	// indexed, once it's scanned
	_, err = update(t, r, "media", url.Values{indexer.IndexParam: {"objects"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/a/b"}, indexedFolders(t, r, "media"), "its folders are kept")
	require.Eventually(t, func() bool {
		v, err := r.GetVolume(ctx, "media")
		if err != nil || v.Index == nil || v.Index.Mode != "objects" {
			return false
		}
		_, err = s.GetObject(ctx, "media", "/a/photo.png")
		return err == nil
	}, mountTimeout, 5*time.Millisecond, "its file is indexed again")
}

func Test_Registry_ListCount(t *testing.T) {
	r, _, dir := newIndexedRegistry(t)
	ctx := context.Background()
	u := indexedURL(t, dir, "media", "objects")
	for _, name := range []string{"a/x.txt", "a/b/y.txt", "z.txt"} {
		require.NoError(t, os.WriteFile(filepath.Join(filepath.FromSlash(u.Path), filepath.FromSlash(name)), []byte(name), 0o644))
	}
	_, err := add(t, r, "media", u)
	require.NoError(t, err)
	ready(t, r, "media")
	count := func(name, kind string) *uint64 {
		t.Helper()
		limit := uint64(1)
		list, err := r.List(ctx, schema.ObjectListRequest{ListObjectsRequest: schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Name: name, Path: "/"}, Recursive: true, Type: kind}, Limit: &limit})
		require.NoError(t, err)
		return list.Count
	}

	// A listing of an index is counted, of every page, and of its type
	require.Eventually(t, func() bool {
		n := count("media", "")
		return n != nil && *n == 5
	}, 10*time.Second, 10*time.Millisecond, "a/, a/b/, a/b/y.txt, a/x.txt and z.txt")
	if n := count("media", "text"); assert.NotNil(t, n) {
		assert.Equal(t, uint64(3), *n, "of its type")
	}

	// A listing of a volume which isn't indexed isn't counted, which is null
	_, err = add(t, r, "plain", indexedURL(t, dir, "plain", ""))
	require.NoError(t, err)
	limit := uint64(1)
	list, err := r.List(ctx, schema.ObjectListRequest{ListObjectsRequest: schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Name: "plain", Path: "/"}}, Limit: &limit})
	require.NoError(t, err)
	assert.Nil(t, list.Count)
	data, err := json.Marshal(list)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"count":null`)
}

// A volume's index state is read of the request's context, so its store's
// spans are part of the request's trace, rather than traces of their own
func Test_Registry_ViewTraced(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	tracer := provider.Tracer("test")
	dir := t.TempDir()
	s, err := sqlite.New(context.Background(), gosqlite.Memory, store.WithTracer(tracer))
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	r, err := New(context.Background(), WithDebounce(0), WithBroadcaster(newBroadcaster(t)), WithStore(s))
	require.NoError(t, err)
	t.Cleanup(func() { r.close() })
	runRegistry(t, r)
	_, err = add(t, r, "media", indexedURL(t, dir, "media", "folders"))
	require.NoError(t, err)
	ready(t, r, "media")

	ctx, span := tracer.Start(context.Background(), "request")
	_, err = r.ListVolumes(ctx)
	span.End()
	require.NoError(t, err)
	var states int
	for _, s := range recorder.Ended() {
		if strings.HasSuffix(s.Name(), ".State") && s.SpanContext().TraceID() == span.SpanContext().TraceID() {
			states++
		}
	}
	assert.NotZero(t, states, "the index's state is read in the request's trace")
}
