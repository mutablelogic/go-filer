// Package test is the conformance tests of an index store, which each
// store runs, so they behave the same
package test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	broadcaster "github.com/mutablelogic/go-server/broadcaster"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Run runs the conformance tests of a store, which newStore returns, empty,
// for each test: of its volumes, and their indexes
func Run(t *testing.T, newStore func(t *testing.T, opts ...store.Opt) store.Store) {
	RunVolumes(t, newStore)
	t.Run("Index", func(t *testing.T) { testIndex(t, newStore(t)) })
	t.Run("Get", func(t *testing.T) { testGet(t, withVolumes(t, newStore(t))) })
	t.Run("List", func(t *testing.T) { testList(t, withVolumes(t, newStore(t))) })
	t.Run("Delete", func(t *testing.T) { testDelete(t, withVolumes(t, newStore(t))) })
	t.Run("Volumes", func(t *testing.T) { testVolumes(t, withVolumes(t, newStore(t))) })
	t.Run("DeleteVolume", func(t *testing.T) { testDeleteVolume(t, withVolumes(t, newStore(t))) })
	t.Run("Queue", func(t *testing.T) { testQueue(t, withVolumes(t, newStore(t))) })
	t.Run("QueueCoalesce", func(t *testing.T) { testQueueCoalesce(t, withVolumes(t, newStore(t))) })
	t.Run("QueueOrder", func(t *testing.T) { testQueueOrder(t, withVolumes(t, newStore(t))) })
	t.Run("QueueLease", func(t *testing.T) { testQueueLease(t, withVolumes(t, newStore(t))) })
	t.Run("QueuePending", func(t *testing.T) { testQueuePending(t, withVolumes(t, newStore(t))) })
	t.Run("QueuePendingBeneath", func(t *testing.T) { testQueuePendingBeneath(t, withVolumes(t, newStore(t))) })
	t.Run("Revisions", func(t *testing.T) { testRevisions(t, withVolumes(t, newStore(t))) })
	t.Run("ListType", func(t *testing.T) { testListType(t, withVolumes(t, newStore(t))) })
	t.Run("Scan", func(t *testing.T) { testScan(t, withVolumes(t, newStore(t))) })
	t.Run("SetMetadata", func(t *testing.T) { testSetMetadata(t, withVolumes(t, newStore(t))) })
	t.Run("QueueStale", func(t *testing.T) { testQueueStale(t, withVolumes(t, newStore(t))) })
	t.Run("DeleteMetadata", func(t *testing.T) { testDeleteMetadata(t, withVolumes(t, newStore(t))) })
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// RunVolumes runs the conformance tests of a store's volumes, and the
// state of their indexes, without their objects
func RunVolumes(t *testing.T, newStore func(t *testing.T, opts ...store.Opt) store.Store) {
	t.Run("Volume", func(t *testing.T) { testVolume(t, newStore(t)) })
	t.Run("State", func(t *testing.T) { testState(t, withVolumes(t, newStore(t))) })
	t.Run("Changes", func(t *testing.T) { testChanges(t, newStore) })
}

func testChanges(t *testing.T, newStore func(t *testing.T, opts ...store.Opt) store.Store) {
	ctx := context.Background()
	b := broadcaster.New()
	ch := make(chan changes.Change, 16)
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	_, err := b.Watch(watchCtx, changes.ChangeFilter{Schema: schema.StoreChangeSchema}, ch)
	require.NoError(t, err)
	s := newStore(t, store.WithBroadcaster(b))

	// next returns the change which was published, or nil when none was
	next := func() *changes.Change {
		select {
		case change := <-ch:
			return &change
		default:
			return nil
		}
	}

	// A volume is created, and then updated, when it changes, but not when
	// it doesn't
	volume := schema.VolumeMeta{Name: "media", URL: "file://media/tmp/media?index=folders", Enabled: true}
	_, err = s.CreateVolume(ctx, volume)
	require.NoError(t, err)
	change := next()
	require.NotNil(t, change)
	assert.Equal(t, changes.ChangeCreate, change.Action)
	assert.Equal(t, schema.VolumeChangeTable, change.Table)
	assert.Equal(t, "media", change.Row)
	var data schema.Volume
	require.NoError(t, json.Unmarshal(change.Data, &data))
	assert.Equal(t, volume, data.VolumeMeta)
	_, err = s.UpdateVolume(ctx, volume)
	require.NoError(t, err)
	assert.Nil(t, next(), "it's unchanged")
	volume.Enabled = false
	_, err = s.UpdateVolume(ctx, volume)
	require.NoError(t, err)
	if change := next(); assert.NotNil(t, change) {
		assert.Equal(t, changes.ChangeUpdate, change.Action)
		assert.Equal(t, "media", change.Row)
	}

	// Its state isn't a change of it, and nor is a create, or an update,
	// which fails
	require.NoError(t, s.SetState(ctx, "media", schema.IndexState{Status: schema.IndexStatusReady}))
	_, err = s.CreateVolume(ctx, schema.VolumeMeta{Name: "not a name", URL: "file://x/tmp"})
	assert.Error(t, err)
	_, err = s.CreateVolume(ctx, volume)
	assert.ErrorIs(t, err, gofiler.ErrConflict)
	_, err = s.UpdateVolume(ctx, schema.VolumeMeta{Name: "missing", URL: "file://missing/tmp"})
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	assert.Nil(t, next())

	// It's deleted, which is published, without its data, but not a volume
	// which isn't there
	require.NoError(t, s.DeleteVolume(ctx, "media"))
	if change := next(); assert.NotNil(t, change) {
		assert.Equal(t, changes.ChangeDelete, change.Action)
		assert.Equal(t, "media", change.Row)
		assert.Empty(t, change.Data)
	}
	assert.ErrorIs(t, s.DeleteVolume(ctx, "media"), gofiler.ErrNotFound)
	assert.Nil(t, next())
}

// withVolumes returns the store, with the volumes media and photos
func withVolumes(t *testing.T, s store.Store) store.Store {
	t.Helper()
	for _, name := range []string{"media", "photos"} {
		_, err := s.CreateVolume(context.Background(), schema.VolumeMeta{Name: name, URL: "file://" + name + "/tmp/" + name + "?index=folders", Enabled: true})
		require.NoError(t, err)
	}
	return s
}

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

// keys returns the keys of the objects
func keys(objects []*schema.Object) []string {
	result := []string{}
	for _, object := range objects {
		result = append(result, store.Key(object))
	}
	return result
}

func testVolume(t *testing.T, s store.Store) {
	ctx := context.Background()

	// A volume which isn't stored isn't found, and has no objects, or state,
	// until it is
	_, err := s.GetVolume(ctx, "media")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	assert.ErrorIs(t, s.SetState(ctx, "media", schema.IndexState{Status: schema.IndexStatusReady}), gofiler.ErrNotFound)
	assert.ErrorIs(t, s.DeleteVolume(ctx, "media"), gofiler.ErrNotFound)
	volumes, err := s.ListVolumes(ctx)
	require.NoError(t, err)
	assert.Empty(t, volumes)

	// A volume needs a name, which is an identifier, and a URL, and one
	// which isn't stored can't be updated
	for _, bad := range []schema.VolumeMeta{{URL: "file://x/tmp"}, {Name: "not a name", URL: "file://x/tmp"}, {Name: "media"}} {
		_, err := s.CreateVolume(ctx, bad)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, bad)
		_, err = s.UpdateVolume(ctx, bad)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, bad)
	}
	_, err = s.UpdateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/mnt/media"})
	assert.ErrorIs(t, err, gofiler.ErrNotFound)

	// It's stored, as it's created, and not modified, and can't be created
	// again
	put := schema.VolumeMeta{Name: "media", URL: "file://media/mnt/media?readonly=true", Enabled: true}
	created, err := s.CreateVolume(ctx, put)
	require.NoError(t, err)
	assert.Equal(t, put.Name, created.Name)
	assert.Equal(t, put.URL, created.URL)
	assert.True(t, created.Enabled)
	assert.False(t, created.Created.IsZero())
	assert.True(t, created.Modified.IsZero())
	got, err := s.GetVolume(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, created.URL, got.URL)
	assert.True(t, created.Created.Equal(got.Created))
	_, err = s.CreateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/elsewhere"})
	assert.ErrorIs(t, err, gofiler.ErrConflict)
	got, err = s.GetVolume(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, put.URL, got.URL, "it isn't replaced")

	// Updating it, as it is, doesn't modify it, and changing it does, but
	// keeps when it was created, and its state
	require.NoError(t, s.SetState(ctx, "media", schema.IndexState{Status: schema.IndexStatusReady}))
	again, err := s.UpdateVolume(ctx, put)
	require.NoError(t, err)
	assert.True(t, again.Modified.IsZero())
	put.Enabled, put.URL = false, "file://media/mnt/media?index=folders"
	updated, err := s.UpdateVolume(ctx, put)
	require.NoError(t, err)
	assert.False(t, updated.Enabled)
	assert.Equal(t, put.URL, updated.URL)
	assert.True(t, created.Created.Equal(updated.Created))
	assert.False(t, updated.Modified.IsZero())
	state, err := s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusReady, state.Status, "its state is kept")

	// The volumes are listed in the order of their names
	_, err = s.CreateVolume(ctx, schema.VolumeMeta{Name: "audio", URL: "file://audio/mnt/audio"})
	require.NoError(t, err)
	volumes, err = s.ListVolumes(ctx)
	require.NoError(t, err)
	names := []string{}
	for _, volume := range volumes {
		names = append(names, volume.Name)
	}
	assert.Equal(t, []string{"audio", "media"}, names)

	// A volume is deleted, with its state, and can't be updated, but is
	// created again, without it
	require.NoError(t, s.DeleteVolume(ctx, "media"))
	_, err = s.GetVolume(ctx, "media")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	state, err = s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, state.Status)
	_, err = s.UpdateVolume(ctx, put)
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, err = s.CreateVolume(ctx, put)
	require.NoError(t, err)
	state, err = s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, state.Status)
}

func testIndex(t *testing.T, s store.Store) {
	ctx := context.Background()

	// A volume's objects are stored once it's been created, and kept when
	// it's updated
	assert.ErrorIs(t, s.PutObject(ctx, "media", folder("/a")), gofiler.ErrNotFound)
	volume := schema.VolumeMeta{Name: "media", URL: "file://media/tmp/media", Enabled: true}
	_, err := s.CreateVolume(ctx, volume)
	require.NoError(t, err)
	require.NoError(t, s.PutObject(ctx, "media", folder("/a")))
	volume.Enabled = false
	_, err = s.UpdateVolume(ctx, volume)
	require.NoError(t, err)
	_, err = s.GetObject(ctx, "media", "/a/")
	assert.NoError(t, err, "its index is kept")
}

func testDeleteVolume(t *testing.T, s store.Store) {
	ctx := context.Background()
	require.NoError(t, s.PutObject(ctx, "media", folder("/"), folder("/a")))
	require.NoError(t, s.PutObject(ctx, "photos", folder("/b")))
	require.NoError(t, s.SetState(ctx, "media", schema.IndexState{Status: schema.IndexStatusReady, Folders: 2}))

	// A volume is deleted with its index, and its state, but not another's
	require.NoError(t, s.DeleteVolume(ctx, "media"))
	_, err := s.GetVolume(ctx, "media")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, err = s.GetObject(ctx, "media", "/a/")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	state, err := s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, state.Status)
	_, err = s.GetObject(ctx, "photos", "/b/")
	assert.NoError(t, err)

	// Created again, it has no index, or state
	_, err = s.CreateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/tmp/media"})
	require.NoError(t, err)
	objects, err := s.ListObjects(ctx, "media", store.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	assert.Empty(t, objects)
	state, err = s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, state.Status)
}

func testGet(t *testing.T, s store.Store) {
	ctx := context.Background()

	// An object which isn't indexed isn't found, and a key which isn't
	// absolute, and clean, is bad
	_, err := s.GetObject(ctx, "media", "/a/")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	for _, bad := range []string{"", "a", "/a//", "/a/../b", "/a/./"} {
		_, err := s.GetObject(ctx, "media", bad)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, bad)
	}

	// An object is got as it was put, of its key, with its attributes and
	// metadata
	etag := "etag-1"
	modtime := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	put := folder("/a")
	put.ETag, put.ModTime, put.ContentType = &etag, modtime, "inode/directory"
	put.Meta = []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"A"`)}}
	require.NoError(t, s.PutObject(ctx, "media", put))
	got, err := s.GetObject(ctx, "media", "/a/")
	require.NoError(t, err)
	assert.Equal(t, "/a", got.Path)
	assert.True(t, got.IsDir)
	assert.Equal(t, etag, *got.ETag)
	assert.True(t, modtime.Equal(got.ModTime))
	assert.Equal(t, "inode/directory", got.ContentType)
	assert.JSONEq(t, `[{"namespace":"dc","key":"title","value":"A"}]`, string(mustJSON(t, got.Meta)))

	// A file of the same path, which a bucket can have, is another object,
	// with its own metadata
	_, err = s.GetObject(ctx, "media", "/a")
	assert.ErrorIs(t, err, gofiler.ErrNotFound, "it's the folder's key")
	same := file("/a")
	same.Meta = []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"file"`)}}
	require.NoError(t, s.PutObject(ctx, "media", same))
	got, err = s.GetObject(ctx, "media", "/a")
	require.NoError(t, err)
	assert.False(t, got.IsDir)
	assert.JSONEq(t, `"file"`, string(got.Meta[0].Value))
	got, err = s.GetObject(ctx, "media", "/a/")
	require.NoError(t, err)
	assert.True(t, got.IsDir)
	assert.JSONEq(t, `"A"`, string(got.Meta[0].Value), "the folder's is kept")

	// The object which was put isn't changed by changing what's returned,
	// or what was put
	got.Path, *got.ETag = "/changed", "changed"
	got.Meta[0].Value[1] = 'X'
	*put.ETag = "changed"
	put.Meta[0].Value[1] = 'Y'
	again, err := s.GetObject(ctx, "media", "/a/")
	require.NoError(t, err)
	assert.Equal(t, "/a", again.Path)
	assert.Equal(t, "etag-1", *again.ETag)
	assert.JSONEq(t, `"A"`, string(again.Meta[0].Value), "nor are its metadata's values")

	// It's replaced when it's put again
	replaced := folder("/a")
	replaced.Meta = nil
	require.NoError(t, s.PutObject(ctx, "media", replaced))
	again, err = s.GetObject(ctx, "media", "/a/")
	require.NoError(t, err)
	assert.Nil(t, again.ETag)
	assert.Empty(t, again.Meta)

	// An object whose key isn't clean is bad, and none of the objects put
	// with it are stored
	for _, bad := range []*schema.Object{file("/b/../c"), folder("/b/."), file("b"), file("")} {
		assert.ErrorIs(t, s.PutObject(ctx, "media", file("/ok"), bad), gofiler.ErrBadParameter, bad.Path)
	}
	_, err = s.GetObject(ctx, "media", "/ok")
	assert.ErrorIs(t, err, gofiler.ErrNotFound, "the batch is rolled back")

	// The root's key is "/"
	require.NoError(t, s.PutObject(ctx, "media", folder("/")))
	root, err := s.GetObject(ctx, "media", "/")
	require.NoError(t, err)
	assert.Equal(t, "/", root.Path)
	assert.True(t, root.IsDir)
}

func testList(t *testing.T, s store.Store) {
	ctx := context.Background()
	require.NoError(t, s.PutObject(ctx, "media",
		folder("/"), file("/a.txt"), folder("/a"), file("/a0.txt"), folder("/a/b"), file("/a/b/c.txt"), folder("/ab"), file("/ab"),
	))

	// A folder's objects, in the order of their keys, as S3 lists them,
	// without the folder, or those beneath its folders
	list := func(req store.ListRequest) []string {
		t.Helper()
		objects, err := s.ListObjects(ctx, "media", req)
		require.NoError(t, err)
		return keys(objects)
	}
	assert.Equal(t, []string{"/a.txt", "/a/", "/a0.txt", "/ab", "/ab/"}, list(store.ListRequest{Parent: "/"}))
	assert.Equal(t, []string{"/a/b/"}, list(store.ListRequest{Parent: "/a/"}))
	assert.Equal(t, []string{}, list(store.ListRequest{Parent: "/ab/"}))
	assert.Equal(t, []string{}, list(store.ListRequest{Parent: "/missing/"}))

	// Every object beneath it, when it's recursive
	assert.Equal(t, []string{"/a.txt", "/a/", "/a/b/", "/a/b/c.txt", "/a0.txt", "/ab", "/ab/"}, list(store.ListRequest{Parent: "/", Recursive: true}))
	assert.Equal(t, []string{"/a/b/", "/a/b/c.txt"}, list(store.ListRequest{Parent: "/a/", Recursive: true}))

	// A page at a time, after the key of the last object returned
	page := list(store.ListRequest{Parent: "/", Limit: 2})
	assert.Equal(t, []string{"/a.txt", "/a/"}, page)
	assert.Equal(t, []string{"/a0.txt", "/ab"}, list(store.ListRequest{Parent: "/", After: "/a/", Limit: 2}))
	assert.Equal(t, []string{"/a/b/c.txt", "/a0.txt"}, list(store.ListRequest{Parent: "/", Recursive: true, After: "/a/b/", Limit: 2}))
	assert.Equal(t, []string{}, list(store.ListRequest{Parent: "/", After: "/ab/"}))

	// A listing is counted, of every page, without where it starts after,
	// or its limit
	for _, req := range []store.ListRequest{{Parent: "/"}, {Parent: "/", After: "/a/", Limit: 1}, {Parent: "/a/"}, {Parent: "/", Recursive: true, Limit: 2}, {Parent: "/missing/"}} {
		count, err := s.CountObjects(ctx, "media", req)
		require.NoError(t, err)
		all := req
		all.After, all.Limit = "", 0
		objects, err := s.ListObjects(ctx, "media", all)
		require.NoError(t, err)
		assert.Equal(t, uint64(len(objects)), count, req)
	}
	_, err := s.CountObjects(ctx, "media", store.ListRequest{Parent: "/a"})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)

	// A parent which isn't a folder's key is bad, as a file's would list
	// what's beside it
	for _, bad := range []string{"/a", "a/", "", "/a/../"} {
		_, err := s.ListObjects(ctx, "media", store.ListRequest{Parent: bad, Recursive: true})
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, bad)
	}
}

func testDelete(t *testing.T, s store.Store) {
	ctx := context.Background()
	require.NoError(t, s.PutObject(ctx, "media", folder("/"), folder("/a"), folder("/a/b"), file("/a/b/c.txt"), folder("/ab"), file("/a.txt"), file("/a")))

	// A folder is deleted with what's beneath it, but not a key which only
	// starts with its path, or a file of its path
	require.NoError(t, s.DeleteObject(ctx, "media", "/a/"))
	objects, err := s.ListObjects(ctx, "media", store.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"/a", "/a.txt", "/ab/"}, keys(objects))
	_, err = s.GetObject(ctx, "media", "/a/b/c.txt")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)

	// A file is deleted on its own
	require.NoError(t, s.PutObject(ctx, "media", folder("/a"), file("/a/b.txt")))
	require.NoError(t, s.DeleteObject(ctx, "media", "/a"))
	objects, err = s.ListObjects(ctx, "media", store.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"/a.txt", "/a/", "/a/b.txt", "/ab/"}, keys(objects))

	// Deleting what isn't there isn't an error, a bad key is, and the root
	// deletes it all
	require.NoError(t, s.DeleteObject(ctx, "media", "/missing"))
	assert.ErrorIs(t, s.DeleteObject(ctx, "media", "a"), gofiler.ErrBadParameter)
	require.NoError(t, s.DeleteObject(ctx, "media", "/"))
	objects, err = s.ListObjects(ctx, "media", store.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	assert.Empty(t, objects)
	_, err = s.GetObject(ctx, "media", "/")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
}

func testVolumes(t *testing.T, s store.Store) {
	ctx := context.Background()

	// Each volume's objects are its own
	require.NoError(t, s.PutObject(ctx, "media", folder("/a")))
	require.NoError(t, s.PutObject(ctx, "photos", folder("/b")))
	_, err := s.GetObject(ctx, "photos", "/a/")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	objects, err := s.ListObjects(ctx, "photos", store.ListRequest{Parent: "/"})
	require.NoError(t, err)
	assert.Equal(t, []string{"/b"}, paths(objects))
	require.NoError(t, s.DeleteObject(ctx, "photos", "/"))
	_, err = s.GetObject(ctx, "media", "/a/")
	assert.NoError(t, err)
}

func testState(t *testing.T, s store.Store) {
	ctx := context.Background()

	// A volume which isn't indexed has no status
	state, err := s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, state.Status)

	// Its state is stored, of each volume
	started := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	want := schema.IndexState{Status: schema.IndexStatusReady, Folders: 12, Files: 34, Mode: "objects", Phase: schema.IndexPhaseIndexing, Started: started, Scanned: started.Add(time.Minute), Error: "it failed"}
	require.NoError(t, s.SetState(ctx, "media", want))
	state, err = s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, want.Status, state.Status)
	assert.Equal(t, want.Folders, state.Folders)
	assert.Equal(t, want.Files, state.Files)
	assert.Equal(t, want.Mode, state.Mode)
	assert.Equal(t, want.Phase, state.Phase)
	assert.True(t, want.Started.Equal(state.Started))
	assert.True(t, want.Scanned.Equal(state.Scanned))
	assert.Equal(t, want.Error, state.Error)
	other, err := s.State(ctx, "photos")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, other.Status)
	unknown, err := s.State(ctx, "audio")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, unknown.Status, "nor has a volume which isn't stored")

	// A zero state clears it
	require.NoError(t, s.SetState(ctx, "media", schema.IndexState{}))
	state, err = s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexState{}, state)
}

// mustJSON returns the JSON of a value
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return data
}

// all is every kind of work of indexing
var all = []store.Kind{store.KindIndex, store.KindRescan}

// claimed returns the keys of the work which is claimed, of the kinds
func claimed(t *testing.T, s store.Store, kinds []store.Kind, max int) ([]store.Work, []string) {
	t.Helper()
	work, err := s.Claim(context.Background(), "media", kinds, max, time.Hour)
	require.NoError(t, err)
	result := []string{}
	for _, w := range work {
		result = append(result, string(w.Kind)+" "+w.Key)
	}
	return work, result
}

func index(key string) store.Work  { return store.Work{Key: key, Kind: store.KindIndex} }
func rescan(key string) store.Work { return store.Work{Key: key, Kind: store.KindRescan} }

func testQueue(t *testing.T, s store.Store) {
	ctx := context.Background()

	// Work of a volume which isn't stored isn't queued
	assert.ErrorIs(t, s.Push(ctx, "missing", index("/a")), gofiler.ErrNotFound)

	// Work is claimed, once, until it's done, and is the volume's own
	require.NoError(t, s.Push(ctx, "media", index("/a")))
	require.NoError(t, s.Push(ctx, "photos", index("/b")))
	work, paths := claimed(t, s, all, 10)
	assert.Equal(t, []string{"index /a"}, paths)
	assert.False(t, work[0].Leased.IsZero())
	_, paths = claimed(t, s, all, 10)
	assert.Empty(t, paths, "it's leased")
	require.NoError(t, s.Done(ctx, "media", work...))
	_, paths = claimed(t, s, all, 10)
	assert.Empty(t, paths, "it's done")

	// Work which is pushed again, while it's claimed, isn't done, and is
	// claimed again, when the push is due
	require.NoError(t, s.Push(ctx, "media", index("/a")))
	work, _ = claimed(t, s, all, 10)
	require.NoError(t, s.Push(ctx, "media", store.Work{Key: "/a", Kind: store.KindIndex, After: time.Now().Add(time.Hour)}))
	require.NoError(t, s.Done(ctx, "media", work...))
	_, paths = claimed(t, s, all, 10)
	assert.Empty(t, paths, "it's due when the push is")
	require.NoError(t, s.Push(ctx, "media", index("/a")))
	work, paths = claimed(t, s, all, 10)
	assert.Equal(t, []string{"index /a"}, paths, "it's pushed again")

	// Work which fails is due again after its retry, with why, and the
	// attempts
	require.NoError(t, s.Failed(ctx, "media", work[0], errors.New("it failed"), 0))
	work, paths = claimed(t, s, all, 10)
	require.Equal(t, []string{"index /a"}, paths)
	assert.Equal(t, uint(1), work[0].Attempts)
	assert.Equal(t, "it failed", work[0].Error)
	require.NoError(t, s.Failed(ctx, "media", work[0], errors.New("again"), time.Hour))
	_, paths = claimed(t, s, all, 10)
	assert.Empty(t, paths, "it's due later")

	// Of the kinds which are claimed, and none of none
	require.NoError(t, s.Push(ctx, "media", rescan("/r/")))
	_, paths = claimed(t, s, []store.Kind{store.KindIndex}, 10)
	assert.Empty(t, paths)
	_, paths = claimed(t, s, nil, 10)
	assert.Empty(t, paths)
	_, paths = claimed(t, s, []store.Kind{store.KindRescan}, 10)
	assert.Equal(t, []string{"rescan /r/"}, paths)

	// Deleting the volume deletes its work
	require.NoError(t, s.Push(ctx, "media", index("/z")))
	require.NoError(t, s.DeleteVolume(ctx, "media"))
	_, err := s.CreateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/tmp/media"})
	require.NoError(t, err)
	_, paths = claimed(t, s, all, 10)
	assert.Empty(t, paths)
}

func testQueueCoalesce(t *testing.T, s store.Store) {
	ctx := context.Background()

	// Work of the same key, and kind, is queued once
	require.NoError(t, s.Push(ctx, "media", index("/a/b"), index("/a/b"), index("/a/c"), index("/d")))

	// A rescan of a folder replaces the work beneath it, and the index of
	// its own key, but not that of other folders, including those whose
	// paths only start with its path, or of a file of its path
	require.NoError(t, s.Push(ctx, "media", index("/a/"), index("/a"), index("/ab"), rescan("/a/x/")))
	require.NoError(t, s.Push(ctx, "media", rescan("/a/")))

	// Work beneath a rescan which is queued, or of its own key, isn't
	// queued
	require.NoError(t, s.Push(ctx, "media", index("/a/e"), rescan("/a/f/"), index("/a/")))
	_, paths := claimed(t, s, []store.Kind{store.KindIndex}, 10)
	assert.ElementsMatch(t, []string{"index /a", "index /ab", "index /d"}, paths)
	_, paths = claimed(t, s, []store.Kind{store.KindRescan}, 10)
	assert.Equal(t, []string{"rescan /a/"}, paths)
	_, paths = claimed(t, s, all, 10)
	assert.Empty(t, paths)

	// A rescan of the root, which is a catch-up, doesn't replace the work
	// beneath it, nor is work of it, as the index of a change, and a rescan
	// of a folder, which can have absorbed one, hold up reads, and a
	// catch-up doesn't
	require.NoError(t, s.Push(ctx, "media", index("/x"), rescan("/y/")))
	require.NoError(t, s.Push(ctx, "media", rescan("/")))
	require.NoError(t, s.Push(ctx, "media", index("/z"), rescan("/w/")))
	_, paths = claimed(t, s, []store.Kind{store.KindIndex}, 10)
	assert.ElementsMatch(t, []string{"index /x", "index /z"}, paths)
	var work []store.Work
	rescans := []string{}
	for {
		claim, paths := claimed(t, s, []store.Kind{store.KindRescan}, 10)
		if len(paths) == 0 {
			break
		}
		rescans = append(rescans, paths...)
		if paths[0] == "rescan /" {
			work = claim
		} else {
			require.NoError(t, s.Done(ctx, "media", claim...))
		}
	}
	assert.ElementsMatch(t, []string{"rescan /y/", "rescan /", "rescan /w/"}, rescans)

	// Work beneath a rescan which is claimed is queued, as the rescan can
	// have passed it, and isn't done with it
	require.NoError(t, s.Push(ctx, "media", index("/a/b"), rescan("/c/")))
	require.NoError(t, s.Done(ctx, "media", work...))
	_, paths = claimed(t, s, []store.Kind{store.KindIndex}, 10)
	assert.Equal(t, []string{"index /a/b"}, paths)
	_, paths = claimed(t, s, []store.Kind{store.KindRescan}, 10)
	assert.Equal(t, []string{"rescan /c/"}, paths)

	// Work of a key which isn't absolute, or clean, or without a kind, or a
	// rescan of a file's key, is bad, and none of the work is queued
	for _, bad := range []store.Work{index("a/b"), index(""), index("."), index("/a/../b"), index("/a//"), {Key: "/a"}, rescan("/a")} {
		assert.ErrorIs(t, s.Push(ctx, "media", index("/ok"), bad), gofiler.ErrBadParameter, bad)
	}
	_, paths = claimed(t, s, all, 10)
	assert.Empty(t, paths)
}

func testQueueOrder(t *testing.T, s store.Store) {
	ctx := context.Background()
	now := time.Now()

	// Work is claimed earliest first, a rescan on its own, and other work
	// up to the first rescan, and up to the most which are claimed
	require.NoError(t, s.Push(ctx, "media",
		store.Work{Key: "/b", Kind: store.KindIndex, After: now.Add(-3 * time.Second)},
		store.Work{Key: "/a", Kind: store.KindIndex, After: now.Add(-2 * time.Second)},
		store.Work{Key: "/r/", Kind: store.KindRescan, After: now.Add(-1500 * time.Millisecond)},
		store.Work{Key: "/c", Kind: store.KindIndex, After: now.Add(-time.Second)},
		store.Work{Key: "/d", Kind: store.KindIndex, After: now.Add(-500 * time.Millisecond)},
	))
	_, paths := claimed(t, s, all, 1)
	assert.Equal(t, []string{"index /b"}, paths)
	_, paths = claimed(t, s, all, 10)
	assert.Equal(t, []string{"index /a"}, paths, "up to the rescan")
	_, paths = claimed(t, s, all, 10)
	assert.Equal(t, []string{"rescan /r/"}, paths, "on its own")
	_, paths = claimed(t, s, all, 10)
	assert.Equal(t, []string{"index /c", "index /d"}, paths)

	// Work which isn't due isn't claimed, and is due at the earlier of two
	// pushes, so work which is pushed again and again isn't put off
	require.NoError(t, s.Push(ctx, "media", store.Work{Key: "/later", Kind: store.KindIndex, After: now.Add(time.Hour)}))
	_, paths = claimed(t, s, all, 10)
	assert.Empty(t, paths)
	require.NoError(t, s.Push(ctx, "media", store.Work{Key: "/later", Kind: store.KindIndex, After: now.Add(2 * time.Hour)}))
	_, paths = claimed(t, s, all, 10)
	assert.Empty(t, paths)
	require.NoError(t, s.Push(ctx, "media", store.Work{Key: "/later", Kind: store.KindIndex, After: now}))
	_, paths = claimed(t, s, all, 10)
	assert.Equal(t, []string{"index /later"}, paths)
}

func testQueueLease(t *testing.T, s store.Store) {
	ctx := context.Background()

	// Work whose lease has ended is claimed again, as when its claimer has
	// stopped, and the claim which ended isn't done, or failed, of it
	require.NoError(t, s.Push(ctx, "media", index("/a")))
	first, err := s.Claim(ctx, "media", all, 10, time.Millisecond)
	require.NoError(t, err)
	require.Len(t, first, 1)
	time.Sleep(10 * time.Millisecond)
	second, err := s.Claim(ctx, "media", all, 10, time.Hour)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.NoError(t, s.Done(ctx, "media", first...))
	require.NoError(t, s.Failed(ctx, "media", first[0], errors.New("too late"), 0))
	_, paths := claimed(t, s, all, 10)
	assert.Empty(t, paths, "it's still the second claim's")
	require.NoError(t, s.Done(ctx, "media", second...))
	require.NoError(t, s.Push(ctx, "media", index("/b")))
	_, paths = claimed(t, s, all, 10)
	assert.Equal(t, []string{"index /b"}, paths, "the second claim is done")
}

func testQueuePendingBeneath(t *testing.T, s store.Store) {
	ctx := context.Background()
	pending := func(folder string) bool {
		t.Helper()
		result, err := s.PendingBeneath(ctx, "media", folder)
		require.NoError(t, err)
		return result
	}
	done := func() {
		t.Helper()
		work, _ := claimed(t, s, all, 10)
		require.NoError(t, s.Done(ctx, "media", work...))
	}

	// A folder's listing has work pending when a key beneath it is indexed,
	// or rescanned, whether the work is queued, or claimed, until it's done
	assert.False(t, pending("/a/"))
	require.NoError(t, s.Push(ctx, "media", index("/a/b/c.txt")))
	assert.True(t, pending("/a/"), "of a file beneath it")
	assert.True(t, pending("/a/b/"), "of a file in it")
	assert.True(t, pending("/"), "of the root")
	assert.False(t, pending("/a/b/c/"), "of a folder beneath it")
	assert.False(t, pending("/ab/"), "of a folder whose path only starts with it")
	assert.False(t, pending("/a/bc/"), "of another folder")
	work, _ := claimed(t, s, all, 10)
	assert.True(t, pending("/a/"), "while it's claimed")
	require.NoError(t, s.Done(ctx, "media", work...))
	assert.False(t, pending("/a/"))

	// Or of a rescan beneath it, and of the work Pending is of: an index, or
	// a rescan, of it, or of a folder above it, but not of a file of its
	// path
	require.NoError(t, s.Push(ctx, "media", rescan("/a/b/")))
	assert.True(t, pending("/a/"), "of a rescan beneath it")
	done()
	require.NoError(t, s.Push(ctx, "media", index("/a/")))
	assert.True(t, pending("/a/"), "of an index of it")
	assert.True(t, pending("/a/b/"), "of an index above it")
	assert.False(t, pending("/c/"))
	done()
	require.NoError(t, s.Push(ctx, "media", index("/a")))
	assert.False(t, pending("/a/"), "of a file of its path")
	done()
	require.NoError(t, s.Push(ctx, "media", rescan("/a/")))
	assert.True(t, pending("/a/b/"), "of a rescan above it")
	done()
	require.NoError(t, s.Push(ctx, "media", rescan("/")))
	assert.False(t, pending("/a/b/"), "but not of the root, which is a catch-up")
	assert.False(t, pending("/"), "nor of the root's own")
	done()
	assert.False(t, pending("/"))

	// A key which isn't a folder's is bad
	for _, bad := range []string{"a", "/a"} {
		_, err := s.PendingBeneath(ctx, "media", bad)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, bad)
	}
}

func testQueuePending(t *testing.T, s store.Store) {
	ctx := context.Background()
	pending := func(key string) bool {
		t.Helper()
		result, err := s.Pending(ctx, "media", key)
		require.NoError(t, err)
		return result
	}

	// A key has work pending when it's indexed, or it, or a folder above it,
	// is rescanned, whether the work is queued, or claimed, until it's done
	assert.False(t, pending("/a/b.txt"))
	require.NoError(t, s.Push(ctx, "media", index("/a/b.txt")))
	assert.True(t, pending("/a/b.txt"))
	assert.False(t, pending("/a/c.txt"), "of another key")
	assert.False(t, pending("/a/"), "of the folder of the key")
	work, _ := claimed(t, s, all, 10)
	assert.True(t, pending("/a/b.txt"), "while it's claimed")
	require.NoError(t, s.Done(ctx, "media", work...))
	assert.False(t, pending("/a/b.txt"))
	require.NoError(t, s.Push(ctx, "media", rescan("/a/")))
	assert.True(t, pending("/a/b.txt"), "of a folder above it")
	assert.True(t, pending("/a/"), "of itself")
	assert.False(t, pending("/ab"), "of a key which only starts with its path")
	assert.False(t, pending("/a"), "of a file of its path")
	work, _ = claimed(t, s, all, 10)
	require.NoError(t, s.Done(ctx, "media", work...))
	require.NoError(t, s.Push(ctx, "media", index("/a/")))
	assert.True(t, pending("/a/b.txt"), "of an index of a folder above it")
	assert.False(t, pending("/"), "of the folder above the folder")
	work, _ = claimed(t, s, all, 10)
	require.NoError(t, s.Done(ctx, "media", work...))

	// But not of a rescan of the root, which is a catch-up, which the index
	// is served while it's done, while a change of it, while it's queued, is
	require.NoError(t, s.Push(ctx, "media", rescan("/")))
	assert.False(t, pending("/a/b.txt"), "of a catch-up")
	require.NoError(t, s.Push(ctx, "media", index("/a/b.txt")))
	assert.True(t, pending("/a/b.txt"), "of a change, while a catch-up is queued")
	work, _ = claimed(t, s, all, 10)
	require.NoError(t, s.Done(ctx, "media", work...))
	for len(work) > 0 {
		work, _ = claimed(t, s, all, 10)
		require.NoError(t, s.Done(ctx, "media", work...))
	}

	// Nor is a change, which a rescan of its folder absorbed, forgotten, of a
	// rescan of the root which is pushed after it
	require.NoError(t, s.Push(ctx, "media", index("/a/b.txt")))
	require.NoError(t, s.Push(ctx, "media", rescan("/a/")))
	require.NoError(t, s.Push(ctx, "media", rescan("/")))
	assert.True(t, pending("/a/b.txt"), "of the rescan of its folder")
	_, err := s.Pending(ctx, "media", "a/b")
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}

func testRevisions(t *testing.T, s store.Store) {
	ctx := context.Background()
	require.NoError(t, s.PutObject(ctx, "media", folder("/"), file("/a.txt"), folder("/b"), file("/b/c.txt"), folder("/b/d"), file("/b/d/e.txt"), file("/bc.txt")))
	revisions := func(folder string, recursive bool) map[string]string {
		t.Helper()
		result, err := s.Revisions(ctx, "media", folder, recursive)
		require.NoError(t, err)
		return result
	}

	// The revisions of files are stored, by their keys, and are of a
	// folder's files, or every file beneath it
	assert.Empty(t, revisions("/", true))
	require.NoError(t, s.SetRevisions(ctx, "media", map[string]string{"/a.txt": "1", "/b/c.txt": "2", "/b/d/e.txt": "3", "/bc.txt": "4"}))
	assert.Equal(t, map[string]string{"/a.txt": "1", "/bc.txt": "4"}, revisions("/", false))
	assert.Equal(t, map[string]string{"/b/c.txt": "2"}, revisions("/b/", false))
	assert.Equal(t, map[string]string{"/b/c.txt": "2", "/b/d/e.txt": "3"}, revisions("/b/", true))
	assert.Len(t, revisions("/", true), 4)
	require.NoError(t, s.SetRevisions(ctx, "media", map[string]string{"/a.txt": "5"}))
	assert.Equal(t, "5", revisions("/", false)["/a.txt"], "it's replaced")

	// They aren't work, so aren't claimed, or pending, and a rescan doesn't
	// remove them
	_, paths := claimed(t, s, all, 10)
	assert.Empty(t, paths)
	pending, err := s.Pending(ctx, "media", "/b/c.txt")
	require.NoError(t, err)
	assert.False(t, pending)
	require.NoError(t, s.Push(ctx, "media", rescan("/b/")))
	work, _ := claimed(t, s, all, 10)
	require.NoError(t, s.Done(ctx, "media", work...))
	assert.Equal(t, "2", revisions("/b/", false)["/b/c.txt"])

	// A file which is put again has none, and nor has one which is deleted,
	// or beneath a folder which is
	require.NoError(t, s.PutObject(ctx, "media", file("/a.txt")))
	assert.NotContains(t, revisions("/", false), "/a.txt")
	require.NoError(t, s.DeleteObject(ctx, "media", "/bc.txt"))
	assert.NotContains(t, revisions("/", false), "/bc.txt")
	require.NoError(t, s.DeleteObject(ctx, "media", "/b/"))
	assert.Empty(t, revisions("/", true))

	// A folder's key which isn't one is bad, as is a key, and a volume which
	// isn't stored isn't found
	_, err = s.Revisions(ctx, "media", "/b", false)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	assert.ErrorIs(t, s.SetRevisions(ctx, "media", map[string]string{"a": "1"}), gofiler.ErrBadParameter)
	assert.ErrorIs(t, s.SetRevisions(ctx, "missing", map[string]string{"/a": "1"}), gofiler.ErrNotFound)
}

func testListType(t *testing.T, s store.Store) {
	ctx := context.Background()
	typed := func(object *schema.Object, t string) *schema.Object {
		object.ContentType = t
		return object
	}
	require.NoError(t, s.PutObject(ctx, "media",
		folder("/"), typed(file("/a.jpg"), "image/jpeg"), typed(file("/b.png"), "image/png"),
		typed(file("/c.txt"), "text/plain; charset=utf-8"), folder("/d"), typed(file("/d/e.JPG"), "IMAGE/JPEG"),
		file("/f"), typed(folder("/g"), "application/x-apple-sparsebundle"), typed(file("/h.svg"), "image/svg+xml"),
	))
	all, err := s.ListObjects(ctx, "media", store.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)

	// A listing of a type is of the objects which schema.MatchType says are
	// of it, of their types, without their parameters, or case, of a
	// folder's, inode/directory, when it hasn't one, and of a major type,
	// or a subtype, as well as a media type
	for _, kind := range []string{"image", "image/jpeg", "jpeg", " Image/JPEG; q=1 ", "text/plain", "plain", "text", "inode/directory", "directory", "inode", "svg+xml", "application", "x-apple-sparsebundle", "audio", "image/"} {
		for _, recursive := range []bool{true, false} {
			want := []string{}
			for _, object := range all {
				if (recursive || store.ParentKey(store.Key(object)) == "/") && schema.MatchType(object, kind) {
					want = append(want, store.Key(object))
				}
			}
			objects, err := s.ListObjects(ctx, "media", store.ListRequest{Parent: "/", Recursive: recursive, Type: kind})
			require.NoError(t, err)
			assert.Equal(t, want, keys(objects), "%q recursive=%v", kind, recursive)
			count, err := s.CountObjects(ctx, "media", store.ListRequest{Parent: "/", Recursive: recursive, Type: kind})
			require.NoError(t, err)
			assert.Equal(t, uint64(len(want)), count, "%q recursive=%v is counted", kind, recursive)
		}
	}
}

func testScan(t *testing.T, s store.Store) {
	ctx := context.Background()
	require.NoError(t, s.PutObject(ctx, "media", folder("/"), folder("/a"), file("/a/b.txt"), file("/c.txt"), folder("/d"), file("/d/e.txt")))
	require.NoError(t, s.SetRevisions(ctx, "media", map[string]string{"/a/b.txt": "r1", "/c.txt": "r1", "/d/e.txt": "r1"}))
	changed := func(folder, after string, limit int) []store.Seen {
		t.Helper()
		result, err := s.Changed(ctx, "media", folder, after, limit)
		require.NoError(t, err)
		return result
	}
	count := func(folder string) uint64 {
		t.Helper()
		n, err := s.CountChanged(ctx, "media", folder)
		require.NoError(t, err)
		return n
	}

	// What a scan sees is recorded, and its files which aren't indexed, or
	// whose revisions aren't those they were stored of, have changed, in
	// the order of their keys, a page at a time
	require.NoError(t, s.Seen(ctx, "media", map[string]string{"/a/": "", "/a/b.txt": "r1", "/c.txt": "r2", "/f.txt": "r1"}))
	assert.Equal(t, []store.Seen{{Key: "/c.txt", Revision: "r2", Indexed: true}, {Key: "/f.txt", Revision: "r1"}}, changed("/", "", 0))
	assert.Equal(t, []store.Seen{{Key: "/c.txt", Revision: "r2", Indexed: true}}, changed("/", "", 1))
	assert.Equal(t, []store.Seen{{Key: "/f.txt", Revision: "r1"}}, changed("/", "/c.txt", 1))
	assert.Equal(t, uint64(2), count("/"))
	assert.Empty(t, changed("/a/", "", 0), "of a folder whose files haven't")
	assert.Equal(t, uint64(0), count("/a/"))

	// It isn't work, so it isn't claimed, or pending
	_, paths := claimed(t, s, all, 10)
	assert.Empty(t, paths)
	pending, err := s.Pending(ctx, "media", "/c.txt")
	require.NoError(t, err)
	assert.False(t, pending)

	// What wasn't seen, beneath the folder, is swept, with what's beneath it,
	// but not the folder
	require.NoError(t, s.Sweep(ctx, "media", "/"))
	objects, err := s.ListObjects(ctx, "media", store.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"/a/", "/a/b.txt", "/c.txt"}, keys(objects))
	_, err = s.GetObject(ctx, "media", "/")
	assert.NoError(t, err, "the folder is kept")

	// What's seen is forgotten, of a folder, but not beside it
	require.NoError(t, s.Seen(ctx, "media", map[string]string{"/ab.txt": "r9"}))
	require.NoError(t, s.Unsee(ctx, "media", "/a/"))
	assert.Equal(t, uint64(3), count("/"), "/c.txt, /f.txt and /ab.txt")
	require.NoError(t, s.Unsee(ctx, "media", "/"))
	assert.Equal(t, uint64(0), count("/"))
	require.NoError(t, s.Sweep(ctx, "media", "/a/"))
	_, err = s.GetObject(ctx, "media", "/a/b.txt")
	assert.ErrorIs(t, err, gofiler.ErrNotFound, "what isn't seen, once it's forgotten, is swept")

	// A key which isn't a folder's is bad, and a volume which isn't stored
	// isn't found
	_, err = s.Changed(ctx, "media", "/a", "", 0)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	assert.ErrorIs(t, s.Sweep(ctx, "media", "/a"), gofiler.ErrBadParameter)
	assert.ErrorIs(t, s.Seen(ctx, "missing", map[string]string{"/a": ""}), gofiler.ErrNotFound)
}

func testSetMetadata(t *testing.T, s store.Store) {
	ctx := context.Background()
	meta := func(entries ...string) []schema.Meta {
		result := []schema.Meta{}
		for i := 0; i < len(entries); i += 3 {
			result = append(result, schema.Meta{Namespace: entries[i], Key: entries[i+1], Value: json.RawMessage(entries[i+2])})
		}
		return result
	}
	withMeta := func(object *schema.Object, entries ...string) *schema.Object {
		object.Meta = meta(entries...)
		return object
	}
	got := func(key string) string {
		t.Helper()
		object, err := s.GetObject(ctx, "media", key)
		require.NoError(t, err)
		return string(mustJSON(t, object.Meta))
	}

	// A task's metadata is merged with the backend's, which it replaces of
	// the same namespace and key, after the backend's
	require.NoError(t, s.PutObject(ctx, "media", folder("/"), withMeta(file("/a.txt"), "dc", "title", `"A"`, "dc", "creator", `"X"`)))
	require.NoError(t, s.SetMetadata(ctx, "media", "/a.txt", "metadata", nil, meta("dc", "title", `"B"`, "exif", "width", `100`)))
	assert.JSONEq(t, `[{"namespace":"dc","key":"creator","value":"X"},{"namespace":"dc","key":"title","value":"B"},{"namespace":"exif","key":"width","value":100}]`, got("/a.txt"))

	// Putting the object replaces the backend's, but not the task's, and the
	// task's replaces its own, and when it has none, the backend's are read
	require.NoError(t, s.PutObject(ctx, "media", withMeta(file("/a.txt"), "dc", "title", `"C"`)))
	assert.JSONEq(t, `[{"namespace":"dc","key":"title","value":"B"},{"namespace":"exif","key":"width","value":100}]`, got("/a.txt"))
	require.NoError(t, s.SetMetadata(ctx, "media", "/a.txt", "metadata", nil, meta("exif", "width", `200`)))
	assert.JSONEq(t, `[{"namespace":"dc","key":"title","value":"C"},{"namespace":"exif","key":"width","value":200}]`, got("/a.txt"))
	require.NoError(t, s.SetMetadata(ctx, "media", "/a.txt", "metadata", nil, nil))
	assert.JSONEq(t, `[{"namespace":"dc","key":"title","value":"C"}]`, got("/a.txt"))

	// A listing is merged too
	require.NoError(t, s.SetMetadata(ctx, "media", "/a.txt", "metadata", nil, meta("dc", "title", `"D"`)))
	objects, err := s.ListObjects(ctx, "media", store.ListRequest{Parent: "/"})
	require.NoError(t, err)
	require.Len(t, objects, 1)
	assert.JSONEq(t, `[{"namespace":"dc","key":"title","value":"D"}]`, string(mustJSON(t, objects[0].Meta)))

	// A task which read a version of the object which isn't its version any
	// more is superseded, and doesn't replace what's found of it
	current, err := s.GetObject(ctx, "media", "/a.txt")
	require.NoError(t, err)
	stale := current.ObjectAttr
	stale.ModTime = stale.ModTime.Add(-time.Hour)
	assert.ErrorIs(t, s.SetMetadata(ctx, "media", "/a.txt", "metadata", &stale, meta("dc", "title", `"Stale"`)), gofiler.ErrPreconditionFailed)
	require.NoError(t, s.SetMetadata(ctx, "media", "/a.txt", "metadata", &current.ObjectAttr, meta("dc", "title", `"Current"`)))
	assert.JSONEq(t, `[{"namespace":"dc","key":"title","value":"Current"}]`, got("/a.txt"))

	// An object which isn't indexed isn't found, and a source which isn't a
	// task's kind is bad, and the task's metadata is deleted with its object
	assert.ErrorIs(t, s.SetMetadata(ctx, "media", "/missing.txt", "metadata", nil, nil), gofiler.ErrNotFound)
	for _, bad := range []string{"", "Metadata", "a b", "1x"} {
		assert.ErrorIs(t, s.SetMetadata(ctx, "media", "/a.txt", bad, nil, nil), gofiler.ErrBadParameter, bad)
	}
	require.NoError(t, s.DeleteObject(ctx, "media", "/a.txt"))
	require.NoError(t, s.PutObject(ctx, "media", file("/a.txt")))
	_, err = s.GetObject(ctx, "media", "/a.txt")
	require.NoError(t, err)
	assert.Equal(t, "null", got("/a.txt"), "its task's metadata was deleted with it")
}

func testQueueStale(t *testing.T, s store.Store) {
	ctx := context.Background()
	tasks := []store.Kind{store.KindMetadata}
	require.NoError(t, s.PutObject(ctx, "media", folder("/"), file("/a.txt"), folder("/b"), file("/b/c.txt"), file("/d.txt")))
	require.NoError(t, s.SetRevisions(ctx, "media", map[string]string{"/a.txt": "r1", "/b/c.txt": "r1"}))

	// The files whose listings' revisions aren't those a task was done of
	// are queued, of the folder, once, and those it hasn't been done of,
	// which have no revision, such as one which is stored of a change, but
	// not folders, or of another kind of work
	require.NoError(t, s.PushStale(ctx, "media", "/", store.KindMetadata))
	require.NoError(t, s.PushStale(ctx, "media", "/", store.KindMetadata))
	_, paths := claimed(t, s, all, 10)
	assert.Empty(t, paths, "it isn't indexing")
	work, paths := claimed(t, s, tasks, 10)
	assert.ElementsMatch(t, []string{"metadata /a.txt", "metadata /b/c.txt", "metadata /d.txt"}, paths)

	// Once it's done, it isn't queued again, until its revision changes, as
	// one without a revision isn't, while it has none
	require.NoError(t, s.Done(ctx, "media", work...))
	require.NoError(t, s.PushStale(ctx, "media", "/", store.KindMetadata))
	_, paths = claimed(t, s, tasks, 10)
	assert.Empty(t, paths)
	require.NoError(t, s.SetRevisions(ctx, "media", map[string]string{"/a.txt": "r2"}))
	require.NoError(t, s.PushStale(ctx, "media", "/", store.KindMetadata))
	work, paths = claimed(t, s, tasks, 10)
	assert.Equal(t, []string{"metadata /a.txt"}, paths)

	// Work which is pushed again, while it's claimed, isn't done, so its
	// revision isn't recorded
	require.NoError(t, s.Push(ctx, "media", store.Work{Key: "/a.txt", Kind: store.KindMetadata}))
	require.NoError(t, s.Done(ctx, "media", work...))
	work, paths = claimed(t, s, tasks, 10)
	assert.Equal(t, []string{"metadata /a.txt"}, paths, "it's pushed again")
	require.NoError(t, s.Done(ctx, "media", work...))

	// A file which changed while its task was done of it, whose revision
	// isn't the one it was claimed of, is stale, once it's done
	require.NoError(t, s.SetRevisions(ctx, "media", map[string]string{"/a.txt": "r3"}))
	require.NoError(t, s.PushStale(ctx, "media", "/", store.KindMetadata))
	work, paths = claimed(t, s, tasks, 10)
	require.Equal(t, []string{"metadata /a.txt"}, paths)
	assert.Equal(t, "r3", work[0].Revision, "of the revision it's claimed of")
	require.NoError(t, s.SetRevisions(ctx, "media", map[string]string{"/a.txt": "r3b"}))
	require.NoError(t, s.Done(ctx, "media", work...))
	require.NoError(t, s.PushStale(ctx, "media", "/", store.KindMetadata))
	work, paths = claimed(t, s, tasks, 10)
	assert.Equal(t, []string{"metadata /a.txt"}, paths, "it changed while it was done")
	require.NoError(t, s.Done(ctx, "media", work...))

	// Of a folder, and not beside it
	require.NoError(t, s.SetRevisions(ctx, "media", map[string]string{"/a.txt": "r3", "/b/c.txt": "r3"}))
	require.NoError(t, s.PushStale(ctx, "media", "/b/", store.KindMetadata))
	_, paths = claimed(t, s, tasks, 10)
	assert.Equal(t, []string{"metadata /b/c.txt"}, paths)

	// A file which is deleted has no work of its task, nor the revision it
	// was done of
	require.NoError(t, s.PushStale(ctx, "media", "/", store.KindMetadata))
	require.NoError(t, s.DeleteObject(ctx, "media", "/a.txt"))
	_, paths = claimed(t, s, tasks, 10)
	assert.Empty(t, paths, "its work is gone, and /b/c.txt's is claimed")

	// Work of indexing isn't a task's, and a folder's key which isn't one is
	// bad
	assert.ErrorIs(t, s.PushStale(ctx, "media", "/", store.KindIndex), gofiler.ErrBadParameter)
	assert.ErrorIs(t, s.PushStale(ctx, "media", "/b", store.KindMetadata), gofiler.ErrBadParameter)
	assert.ErrorIs(t, s.PushStale(ctx, "missing", "/", store.KindMetadata), gofiler.ErrNotFound)
}

func testDeleteMetadata(t *testing.T, s store.Store) {
	ctx := context.Background()
	tasks := []store.Kind{store.KindMetadata}
	backend := file("/a.txt")
	backend.Meta = []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"backend"`)}}
	require.NoError(t, s.PutObject(ctx, "media", folder("/"), backend, file("/b.txt")))
	require.NoError(t, s.PutObject(ctx, "photos", folder("/"), file("/a.txt")))
	require.NoError(t, s.SetRevisions(ctx, "media", map[string]string{"/a.txt": "r1", "/b.txt": "r1"}))
	require.NoError(t, s.SetMetadata(ctx, "media", "/a.txt", "metadata", nil, []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"task"`)}}))
	require.NoError(t, s.SetMetadata(ctx, "photos", "/a.txt", "metadata", nil, []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"photos"`)}}))
	require.NoError(t, s.PushStale(ctx, "media", "/", store.KindMetadata))
	work, _ := claimed(t, s, tasks, 10)
	require.NoError(t, s.Done(ctx, "media", work[:1]...))

	// A task's metadata is removed, of the volume, with its work, and the
	// revisions it was done of, so it's done of every file again, but not
	// the backend's metadata, or another volume's
	require.NoError(t, s.DeleteMetadata(ctx, "media", "metadata"))
	object, err := s.GetObject(ctx, "media", "/a.txt")
	require.NoError(t, err)
	assert.JSONEq(t, `[{"namespace":"dc","key":"title","value":"backend"}]`, string(mustJSON(t, object.Meta)))
	other, err := s.GetObject(ctx, "photos", "/a.txt")
	require.NoError(t, err)
	assert.JSONEq(t, `[{"namespace":"dc","key":"title","value":"photos"}]`, string(mustJSON(t, other.Meta)))
	_, paths := claimed(t, s, tasks, 10)
	assert.Empty(t, paths, "its work is removed")
	require.NoError(t, s.PushStale(ctx, "media", "/", store.KindMetadata))
	_, paths = claimed(t, s, tasks, 10)
	assert.ElementsMatch(t, []string{"metadata /a.txt", "metadata /b.txt"}, paths, "it's done of every file again")
	assert.ErrorIs(t, s.DeleteMetadata(ctx, "media", "Bad"), gofiler.ErrBadParameter)
}
