// Package storetest is the conformance tests of an index store, which each
// store runs, so they behave the same
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	store "github.com/mutablelogic/go-filer/filer/store"
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
	assert.ErrorIs(t, s.Put(ctx, "media", folder("/a")), gofiler.ErrNotFound)
	volume := schema.VolumeMeta{Name: "media", URL: "file://media/tmp/media", Enabled: true}
	_, err := s.CreateVolume(ctx, volume)
	require.NoError(t, err)
	require.NoError(t, s.Put(ctx, "media", folder("/a")))
	volume.Enabled = false
	_, err = s.UpdateVolume(ctx, volume)
	require.NoError(t, err)
	_, err = s.Get(ctx, "media", "/a")
	assert.NoError(t, err, "its index is kept")
}

func testDeleteVolume(t *testing.T, s store.Store) {
	ctx := context.Background()
	require.NoError(t, s.Put(ctx, "media", folder("/"), folder("/a")))
	require.NoError(t, s.Put(ctx, "photos", folder("/b")))
	require.NoError(t, s.SetState(ctx, "media", schema.IndexState{Status: schema.IndexStatusReady, Folders: 2}))

	// A volume is deleted with its index, and its state, but not another's
	require.NoError(t, s.DeleteVolume(ctx, "media"))
	_, err := s.GetVolume(ctx, "media")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, err = s.Get(ctx, "media", "/a")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	state, err := s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, state.Status)
	_, err = s.Get(ctx, "photos", "/b")
	assert.NoError(t, err)

	// Created again, it has no index, or state
	_, err = s.CreateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/tmp/media"})
	require.NoError(t, err)
	objects, err := s.List(ctx, "media", store.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	assert.Empty(t, objects)
	state, err = s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexStatusNone, state.Status)
}

func testGet(t *testing.T, s store.Store) {
	ctx := context.Background()

	// An object which isn't indexed isn't found
	_, err := s.Get(ctx, "media", "/a")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)

	// An object is got as it was put, with its attributes and metadata,
	// and is replaced when it's put again
	etag := "etag-1"
	modtime := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	put := folder("/a")
	put.ETag, put.ModTime, put.ContentType = &etag, modtime, "inode/directory"
	put.Meta = []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"A"`)}}
	require.NoError(t, s.Put(ctx, "media", put))
	got, err := s.Get(ctx, "media", "/a")
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
	got.Meta[0].Value[1] = 'X'
	*put.ETag = "changed"
	put.Meta[0].Value[1] = 'Y'
	again, err := s.Get(ctx, "media", "/a")
	require.NoError(t, err)
	assert.Equal(t, "/a", again.Path)
	assert.Equal(t, "etag-1", *again.ETag)
	assert.JSONEq(t, `"A"`, string(again.Meta[0].Value), "nor are its metadata's values")

	// It's replaced when it's put again
	replaced := folder("/a")
	replaced.Meta = nil
	require.NoError(t, s.Put(ctx, "media", replaced))
	again, err = s.Get(ctx, "media", "/a")
	require.NoError(t, err)
	assert.Nil(t, again.ETag)
	assert.Empty(t, again.Meta)
}

func testList(t *testing.T, s store.Store) {
	ctx := context.Background()
	require.NoError(t, s.Put(ctx, "media",
		folder("/"), file("/a.txt"), folder("/a"), file("/a0.txt"), folder("/a/b"), file("/a/b/c.txt"), folder("/ab"),
	))

	// A folder's objects, in the order of their keys, as S3 lists them,
	// without the folder, or those beneath its folders
	list := func(req store.ListRequest) []string {
		t.Helper()
		objects, err := s.List(ctx, "media", req)
		require.NoError(t, err)
		return paths(objects)
	}
	assert.Equal(t, []string{"/a.txt", "/a", "/a0.txt", "/ab"}, list(store.ListRequest{Parent: "/"}))
	assert.Equal(t, []string{"/a/b"}, list(store.ListRequest{Parent: "/a"}))
	assert.Equal(t, []string{}, list(store.ListRequest{Parent: "/ab"}))
	assert.Equal(t, []string{}, list(store.ListRequest{Parent: "/missing"}))

	// Every object beneath it, when it's recursive
	assert.Equal(t, []string{"/a.txt", "/a", "/a/b", "/a/b/c.txt", "/a0.txt", "/ab"}, list(store.ListRequest{Parent: "/", Recursive: true}))
	assert.Equal(t, []string{"/a/b", "/a/b/c.txt"}, list(store.ListRequest{Parent: "/a", Recursive: true}))

	// A page at a time, after the key of the last object returned
	page := list(store.ListRequest{Parent: "/", Limit: 2})
	assert.Equal(t, []string{"/a.txt", "/a"}, page)
	assert.Equal(t, []string{"/a0.txt", "/ab"}, list(store.ListRequest{Parent: "/", After: store.Key(folder("/a")), Limit: 2}))
	assert.Equal(t, []string{"/a/b/c.txt", "/a0.txt"}, list(store.ListRequest{Parent: "/", Recursive: true, After: store.Key(folder("/a/b")), Limit: 2}))
	assert.Equal(t, []string{}, list(store.ListRequest{Parent: "/", After: store.Key(folder("/ab"))}))
}

func testDelete(t *testing.T, s store.Store) {
	ctx := context.Background()
	require.NoError(t, s.Put(ctx, "media", folder("/"), folder("/a"), folder("/a/b"), file("/a/b/c.txt"), folder("/ab"), file("/a.txt")))

	// A folder is deleted with what's beneath it, but not a path which only
	// starts with its path
	require.NoError(t, s.Delete(ctx, "media", "/a"))
	objects, err := s.List(ctx, "media", store.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"/a.txt", "/ab"}, paths(objects))
	_, err = s.Get(ctx, "media", "/a/b/c.txt")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)

	// Deleting what isn't there isn't an error, and the root deletes it all
	require.NoError(t, s.Delete(ctx, "media", "/missing"))
	require.NoError(t, s.Delete(ctx, "media", "/"))
	objects, err = s.List(ctx, "media", store.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	assert.Empty(t, objects)
	_, err = s.Get(ctx, "media", "/")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
}

func testVolumes(t *testing.T, s store.Store) {
	ctx := context.Background()

	// Each volume's objects are its own
	require.NoError(t, s.Put(ctx, "media", folder("/a")))
	require.NoError(t, s.Put(ctx, "photos", folder("/b")))
	_, err := s.Get(ctx, "photos", "/a")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	objects, err := s.List(ctx, "photos", store.ListRequest{Parent: "/"})
	require.NoError(t, err)
	assert.Equal(t, []string{"/b"}, paths(objects))
	require.NoError(t, s.Delete(ctx, "photos", "/"))
	_, err = s.Get(ctx, "media", "/a")
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
	want := schema.IndexState{Status: schema.IndexStatusReady, Folders: 12, Started: started, Scanned: started.Add(time.Minute), Error: "it failed"}
	require.NoError(t, s.SetState(ctx, "media", want))
	state, err = s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, want.Status, state.Status)
	assert.Equal(t, want.Folders, state.Folders)
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

// claimed returns the paths of the work which is claimed, of the kinds
func claimed(t *testing.T, s store.Store, kinds []store.Kind, max int) ([]store.Work, []string) {
	t.Helper()
	work, err := s.Claim(context.Background(), "media", kinds, max, time.Hour)
	require.NoError(t, err)
	result := []string{}
	for _, w := range work {
		result = append(result, string(w.Kind)+" "+w.Path)
	}
	return work, result
}

func index(path string) store.Work  { return store.Work{Path: path, Kind: store.KindIndex} }
func rescan(path string) store.Work { return store.Work{Path: path, Kind: store.KindRescan} }

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
	// claimed again
	require.NoError(t, s.Push(ctx, "media", index("/a")))
	work, _ = claimed(t, s, all, 10)
	require.NoError(t, s.Push(ctx, "media", index("/a")))
	require.NoError(t, s.Done(ctx, "media", work...))
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
	require.NoError(t, s.Push(ctx, "media", rescan("/r")))
	_, paths = claimed(t, s, []store.Kind{store.KindIndex}, 10)
	assert.Empty(t, paths)
	_, paths = claimed(t, s, nil, 10)
	assert.Empty(t, paths)
	_, paths = claimed(t, s, []store.Kind{store.KindRescan}, 10)
	assert.Equal(t, []string{"rescan /r"}, paths)

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

	// Work of the same path, and kind, is queued once
	require.NoError(t, s.Push(ctx, "media", index("/a/b"), index("/a/b"), index("/a/c"), index("/d")))

	// A rescan of a folder replaces the work beneath it, and the index of
	// its own path, but not that of other folders, including those whose
	// paths only start with its path
	require.NoError(t, s.Push(ctx, "media", index("/a"), index("/ab"), rescan("/a/x")))
	require.NoError(t, s.Push(ctx, "media", rescan("/a")))

	// Work beneath a rescan which is queued, or of its own path, isn't
	// queued
	require.NoError(t, s.Push(ctx, "media", index("/a/e"), rescan("/a/f"), index("/a")))
	_, paths := claimed(t, s, []store.Kind{store.KindIndex}, 10)
	assert.ElementsMatch(t, []string{"index /ab", "index /d"}, paths)
	_, paths = claimed(t, s, []store.Kind{store.KindRescan}, 10)
	assert.Equal(t, []string{"rescan /a"}, paths)
	_, paths = claimed(t, s, all, 10)
	assert.Empty(t, paths)

	// A rescan of the root replaces all the work
	require.NoError(t, s.Push(ctx, "media", index("/x"), rescan("/y")))
	require.NoError(t, s.Push(ctx, "media", rescan("/")))
	work, paths := claimed(t, s, all, 10)
	assert.Equal(t, []string{"rescan /"}, paths)

	// Work beneath a rescan which is claimed is queued, as the rescan can
	// have passed it, and isn't done with it
	require.NoError(t, s.Push(ctx, "media", index("/a/b"), rescan("/c")))
	require.NoError(t, s.Done(ctx, "media", work...))
	_, paths = claimed(t, s, []store.Kind{store.KindIndex}, 10)
	assert.Equal(t, []string{"index /a/b"}, paths)
	_, paths = claimed(t, s, []store.Kind{store.KindRescan}, 10)
	assert.Equal(t, []string{"rescan /c"}, paths)

	// Work of a path which isn't absolute, or clean, or without a kind, is
	// bad, and none of the work is queued
	for _, bad := range []store.Work{index("a/b"), index(""), index("."), index("/a/../b"), index("/a/"), {Path: "/a"}} {
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
		store.Work{Path: "/b", Kind: store.KindIndex, After: now.Add(-3 * time.Second)},
		store.Work{Path: "/a", Kind: store.KindIndex, After: now.Add(-2 * time.Second)},
		store.Work{Path: "/r", Kind: store.KindRescan, After: now.Add(-1500 * time.Millisecond)},
		store.Work{Path: "/c", Kind: store.KindIndex, After: now.Add(-time.Second)},
		store.Work{Path: "/d", Kind: store.KindIndex, After: now.Add(-500 * time.Millisecond)},
	))
	_, paths := claimed(t, s, all, 1)
	assert.Equal(t, []string{"index /b"}, paths)
	_, paths = claimed(t, s, all, 10)
	assert.Equal(t, []string{"index /a"}, paths, "up to the rescan")
	_, paths = claimed(t, s, all, 10)
	assert.Equal(t, []string{"rescan /r"}, paths, "on its own")
	_, paths = claimed(t, s, all, 10)
	assert.Equal(t, []string{"index /c", "index /d"}, paths)

	// Work which isn't due isn't claimed, and is due after the later of two
	// pushes, which is the debounce
	require.NoError(t, s.Push(ctx, "media", store.Work{Path: "/later", Kind: store.KindIndex, After: now.Add(time.Hour)}))
	require.NoError(t, s.Push(ctx, "media", store.Work{Path: "/later", Kind: store.KindIndex, After: now}))
	_, paths = claimed(t, s, all, 10)
	assert.Empty(t, paths)
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
