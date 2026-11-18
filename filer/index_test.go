package filer

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fsevents "github.com/mutablelogic/go-filer/filer/fs/fsevents"
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	memory "github.com/mutablelogic/go-filer/filer/indexer/memory"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// newIndexedRegistry returns a running registry, with an index store, and
// a directory for its volumes
func newIndexedRegistry(t *testing.T) (*Registry, *memory.Store, string) {
	t.Helper()
	dir := t.TempDir()
	store := memory.New()
	r, err := New(context.Background(), WithDebounce(0), WithBroadcaster(newBroadcaster(t)), WithIndexerStore(store))
	require.NoError(t, err)
	t.Cleanup(func() { r.close() })
	runRegistry(t, r)
	return r, store, dir
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
		v, err = r.Volume(volume)
		return err == nil && v.Index != nil && v.Index.Status == string(indexer.StatusReady)
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
	// Without a store, a volume can't be indexed, and the option isn't in
	// its URL
	plain, dir := newRegistry(t)
	_, err := plain.Add(context.Background(), "media", indexedURL(t, dir, "media", "folders"))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = plain.Add(context.Background(), "media", indexedURL(t, dir, "media", ""))
	require.NoError(t, err)
	v, err := plain.Volume("media")
	require.NoError(t, err)
	assert.NotContains(t, v.URL, indexer.IndexParam)
	assert.Nil(t, v.Index)

	// With one, an option which isn't a mode is bad, and a volume without
	// it isn't indexed, which its URL says
	r, _, dir := newIndexedRegistry(t)
	_, err = r.Add(context.Background(), "bad", indexedURL(t, dir, "bad", "everything"))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = r.Add(context.Background(), "plain", indexedURL(t, dir, "plain", ""))
	require.NoError(t, err)
	v, err = r.Volume("plain")
	require.NoError(t, err)
	u, err := url.Parse(v.URL)
	require.NoError(t, err)
	assert.Equal(t, "none", u.Query().Get(indexer.IndexParam))
	assert.Nil(t, v.Index)
	_, indexed := get(r, "plain").(*indexer.Indexer)
	assert.False(t, indexed)
}

func Test_Registry_Indexed(t *testing.T) {
	r, store, dir := newIndexedRegistry(t)

	// A volume with index=folders is indexed, by the registry, and its URL,
	// and index, say so
	_, err := r.Add(context.Background(), "media", indexedURL(t, dir, "media", "folders"))
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
	v, err = r.Update(context.Background(), "media", url.Values{indexer.IndexParam: {"none"}})
	require.NoError(t, err)
	assert.Nil(t, v.Index)
	_, indexed := get(r, "media").(*indexer.Indexer)
	assert.False(t, indexed)
	state, err := store.State(context.Background(), "media")
	require.NoError(t, err)
	assert.Equal(t, indexer.StatusNone, state.Status, "its index is removed")
	_, err = r.Update(context.Background(), "media", url.Values{indexer.IndexParam: {"folders"}})
	require.NoError(t, err)
	ready(t, r, "media")

	// An update of its other options keeps it indexed
	_, err = r.Update(context.Background(), "media", url.Values{"readonly": {"true"}})
	require.NoError(t, err)
	v = ready(t, r, "media")
	assert.True(t, v.Readonly)
	_, err = r.Update(context.Background(), "media", url.Values{indexer.IndexParam: {"everything"}})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)

	// Deleting it removes its index
	require.NoError(t, r.Del(context.Background(), "media"))
	state, err = store.State(context.Background(), "media")
	require.NoError(t, err)
	assert.Equal(t, indexer.StatusNone, state.Status)
	_, err = store.Get(context.Background(), "media", "/")
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
	_, err := r.Add(context.Background(), "media", u)
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
	_, err := r.Add(context.Background(), "plain", u)
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/a/b"}, list("plain", "inode/directory"))
	assert.Equal(t, []string{"/a", "/a/b"}, list("plain", "inode"))
	assert.Equal(t, []string{"/a/photo.png"}, list("plain", "image"))
	assert.Equal(t, []string{"/a", "/a/b", "/a/photo.png"}, list("plain", ""))

	// Of folders, of an indexed volume, it's its index's, once it's ready,
	// which has a folder the backend doesn't, until it's watched
	_, err = r.Add(context.Background(), "media", indexedURL(t, dir, "media", "folders"))
	require.NoError(t, err)
	ready(t, r, "media")
	store := r.indexStore
	require.NoError(t, store.Put(context.Background(), "media", &schema.Object{ObjectKey: schema.ObjectKey{Path: "/only-in-the-index"}, ObjectAttr: schema.ObjectAttr{IsDir: true}}))
	for _, typ := range []string{"inode", "directory", "inode/directory", "inode/directory; charset=binary"} {
		assert.Equal(t, []string{"/a", "/a/b", "/only-in-the-index"}, list("media", typ), typ)
	}
	assert.Equal(t, []string{"/a", "/a/b"}, list("media", ""), "every object is the backend's")
}

func Test_Registry_DelCancelledIndex(t *testing.T) {
	r, store, dir := newIndexedRegistry(t)
	_, err := r.Add(context.Background(), "media", indexedURL(t, dir, "media", "folders"))
	require.NoError(t, err)
	ready(t, r, "media")

	// A Del whose request is cancelled still removes the index, once the
	// volume is closed, before its name is released
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_ = r.Del(cancelled, "media")
	require.Eventually(t, func() bool {
		_, err := r.Volume("media")
		return err != nil
	}, 10*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		r.RLock()
		_, registered := r.backend["media"]
		r.RUnlock()
		return !registered
	}, 10*time.Second, 5*time.Millisecond)
	state, err := store.State(context.Background(), "media")
	require.NoError(t, err)
	assert.Equal(t, indexer.StatusNone, state.Status, "its index is removed before its name is released")
	_, err = store.Get(context.Background(), "media", "/")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
}
