// Package storetest is the conformance tests of an index store, which each
// store runs, so they behave the same
package storetest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Run runs the conformance tests of a store, which newStore returns, empty,
// for each test
func Run(t *testing.T, newStore func(t *testing.T) indexer.Store) {
	t.Run("Get", func(t *testing.T) { testGet(t, newStore(t)) })
	t.Run("List", func(t *testing.T) { testList(t, newStore(t)) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, newStore(t)) })
	t.Run("Volumes", func(t *testing.T) { testVolumes(t, newStore(t)) })
	t.Run("State", func(t *testing.T) { testState(t, newStore(t)) })
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// folder and file return a folder, and a file, of a path
func folder(path string) *schema.Object {
	return &schema.Object{ObjectKey: schema.ObjectKey{Path: path}, ObjectAttr: schema.ObjectAttr{IsDir: true}}
}

func file(path string) *schema.Object {
	size := int64(len(path))
	return &schema.Object{ObjectKey: schema.ObjectKey{Path: path}, ObjectAttr: schema.ObjectAttr{Size: &size}}
}

// paths returns the paths of the objects
func paths(objects []*schema.Object) []string {
	result := []string{}
	for _, object := range objects {
		result = append(result, object.Path)
	}
	return result
}

func testGet(t *testing.T, store indexer.Store) {
	ctx := context.Background()

	// An object which isn't indexed isn't found
	_, err := store.Get(ctx, "media", "/a")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)

	// An object is got as it was put, with its attributes and metadata,
	// and is replaced when it's put again
	etag := "etag-1"
	modtime := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	put := folder("/a")
	put.ETag, put.ModTime, put.ContentType = &etag, modtime, "inode/directory"
	put.Meta = []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"A"`)}}
	require.NoError(t, store.Put(ctx, "media", put))
	got, err := store.Get(ctx, "media", "/a")
	require.NoError(t, err)
	assert.Equal(t, "/a", got.Path)
	assert.True(t, got.IsDir)
	assert.Equal(t, etag, *got.ETag)
	assert.True(t, modtime.Equal(got.ModTime))
	assert.Equal(t, "inode/directory", got.ContentType)
	assert.JSONEq(t, `[{"namespace":"dc","key":"title","value":"A"}]`, string(mustJSON(t, got.Meta)))

	// The object which was put isn't changed by changing what's returned,
	// or what was put
	got.Path, *got.ETag = "/changed", "changed"
	*put.ETag = "changed"
	again, err := store.Get(ctx, "media", "/a")
	require.NoError(t, err)
	assert.Equal(t, "/a", again.Path)
	assert.Equal(t, "etag-1", *again.ETag)

	// It's replaced when it's put again
	replaced := folder("/a")
	replaced.Meta = nil
	require.NoError(t, store.Put(ctx, "media", replaced))
	again, err = store.Get(ctx, "media", "/a")
	require.NoError(t, err)
	assert.Nil(t, again.ETag)
	assert.Empty(t, again.Meta)
}

func testList(t *testing.T, store indexer.Store) {
	ctx := context.Background()
	require.NoError(t, store.Put(ctx, "media",
		folder("/"), file("/a.txt"), folder("/a"), file("/a0.txt"), folder("/a/b"), file("/a/b/c.txt"), folder("/ab"),
	))

	// A folder's objects, in the order of their keys, as S3 lists them,
	// without the folder, or those beneath its folders
	list := func(req indexer.ListRequest) []string {
		t.Helper()
		objects, err := store.List(ctx, "media", req)
		require.NoError(t, err)
		return paths(objects)
	}
	assert.Equal(t, []string{"/a.txt", "/a", "/a0.txt", "/ab"}, list(indexer.ListRequest{Parent: "/"}))
	assert.Equal(t, []string{"/a/b"}, list(indexer.ListRequest{Parent: "/a"}))
	assert.Equal(t, []string{}, list(indexer.ListRequest{Parent: "/ab"}))
	assert.Equal(t, []string{}, list(indexer.ListRequest{Parent: "/missing"}))

	// Every object beneath it, when it's recursive
	assert.Equal(t, []string{"/a.txt", "/a", "/a/b", "/a/b/c.txt", "/a0.txt", "/ab"}, list(indexer.ListRequest{Parent: "/", Recursive: true}))
	assert.Equal(t, []string{"/a/b", "/a/b/c.txt"}, list(indexer.ListRequest{Parent: "/a", Recursive: true}))

	// A page at a time, after the key of the last object returned
	page := list(indexer.ListRequest{Parent: "/", Limit: 2})
	assert.Equal(t, []string{"/a.txt", "/a"}, page)
	assert.Equal(t, []string{"/a0.txt", "/ab"}, list(indexer.ListRequest{Parent: "/", After: indexer.Key(folder("/a")), Limit: 2}))
	assert.Equal(t, []string{"/a/b/c.txt", "/a0.txt"}, list(indexer.ListRequest{Parent: "/", Recursive: true, After: indexer.Key(folder("/a/b")), Limit: 2}))
	assert.Equal(t, []string{}, list(indexer.ListRequest{Parent: "/", After: indexer.Key(folder("/ab"))}))
}

func testDelete(t *testing.T, store indexer.Store) {
	ctx := context.Background()
	require.NoError(t, store.Put(ctx, "media", folder("/"), folder("/a"), folder("/a/b"), file("/a/b/c.txt"), folder("/ab"), file("/a.txt")))

	// A folder is deleted with what's beneath it, but not a path which only
	// starts with its path
	require.NoError(t, store.Delete(ctx, "media", "/a"))
	objects, err := store.List(ctx, "media", indexer.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"/a.txt", "/ab"}, paths(objects))
	_, err = store.Get(ctx, "media", "/a/b/c.txt")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)

	// Deleting what isn't there isn't an error, and the root deletes it all
	require.NoError(t, store.Delete(ctx, "media", "/missing"))
	require.NoError(t, store.Delete(ctx, "media", "/"))
	objects, err = store.List(ctx, "media", indexer.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	assert.Empty(t, objects)
	_, err = store.Get(ctx, "media", "/")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
}

func testVolumes(t *testing.T, store indexer.Store) {
	ctx := context.Background()

	// Each volume's objects are its own
	require.NoError(t, store.Put(ctx, "media", folder("/a")))
	require.NoError(t, store.Put(ctx, "photos", folder("/b")))
	_, err := store.Get(ctx, "photos", "/a")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	objects, err := store.List(ctx, "photos", indexer.ListRequest{Parent: "/"})
	require.NoError(t, err)
	assert.Equal(t, []string{"/b"}, paths(objects))
	require.NoError(t, store.Delete(ctx, "photos", "/"))
	_, err = store.Get(ctx, "media", "/a")
	assert.NoError(t, err)
}

func testState(t *testing.T, store indexer.Store) {
	ctx := context.Background()

	// A volume which isn't indexed has no status
	state, err := store.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, indexer.StatusNone, state.Status)

	// Its state is stored, of each volume
	started := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	want := indexer.State{Status: indexer.StatusReady, Folders: 12, Started: started, Scanned: started.Add(time.Minute), Error: "it failed"}
	require.NoError(t, store.SetState(ctx, "media", want))
	state, err = store.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, want.Status, state.Status)
	assert.Equal(t, want.Folders, state.Folders)
	assert.True(t, want.Started.Equal(state.Started))
	assert.True(t, want.Scanned.Equal(state.Scanned))
	assert.Equal(t, want.Error, state.Error)
	other, err := store.State(ctx, "photos")
	require.NoError(t, err)
	assert.Equal(t, indexer.StatusNone, other.Status)
}

// mustJSON returns the JSON of a value
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return data
}
