package sqlite_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	storetest "github.com/mutablelogic/go-filer/store/test"
	gosqlite "github.com/mutablelogic/go-sqlite"
	ncruces "github.com/mutablelogic/go-sqlite/ncruces"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
	codes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// newStore returns a store, of a database in memory
func newStore(t *testing.T, opts ...store.Opt) store.Store {
	t.Helper()
	s, err := sqlite.New(context.Background(), gosqlite.Memory, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func Test_Store(t *testing.T) {
	// It's as every store is
	storetest.Run(t, newStore)
}

func Test_New(t *testing.T) {
	// It needs its path
	_, err := sqlite.New(context.Background(), "")
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)

	// Its volumes outlast it, in its file
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "index.db")
	s, err := sqlite.New(ctx, path)
	require.NoError(t, err)
	_, err = s.CreateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/mnt/media?index=folders", Enabled: true})
	require.NoError(t, err)
	require.NoError(t, s.Close())
	s, err = sqlite.New(ctx, path)
	require.NoError(t, err)
	defer s.Close()
	volume, err := s.GetVolume(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, "file://media/mnt/media?index=folders", volume.URL)
	assert.True(t, volume.Enabled)
}

func Test_Trace(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())

	// Each method is a span, of its arguments, with its queries' spans
	// within it, and its error
	ctx := context.Background()
	s, err := sqlite.New(ctx, gosqlite.Memory, store.WithTracer(provider.Tracer("test")))
	require.NoError(t, err)
	defer s.Close()
	_, err = s.CreateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/mnt/media?index=none", Enabled: true})
	require.NoError(t, err)
	_, err = s.GetVolume(ctx, "missing")
	require.ErrorIs(t, err, gofiler.ErrNotFound)

	spans := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range recorder.Ended() {
		spans[span.Name()] = span
	}
	for _, name := range []string{"filer.store.sqlite.New", "filer.store.sqlite.CreateVolume", "filer.store.sqlite.GetVolume"} {
		require.Contains(t, spans, name)
	}
	put := spans["filer.store.sqlite.CreateVolume"]
	assert.Contains(t, attrs(put)["arg.volume"], "media")
	assert.Equal(t, codes.Unset, put.Status().Code)
	queries := 0
	for _, span := range recorder.Ended() {
		if span.Parent().SpanID() == put.SpanContext().SpanID() {
			queries++
		}
	}
	assert.NotZero(t, queries, "its queries are within it")
	get := spans["filer.store.sqlite.GetVolume"]
	assert.Equal(t, "missing", attrs(get)["arg.name"])
	assert.Equal(t, codes.Error, get.Status().Code)
}

// attrs returns the attributes of a span, as strings
func attrs(span sdktrace.ReadOnlySpan) map[string]string {
	result := map[string]string{}
	for _, attr := range span.Attributes() {
		result[string(attr.Key)] = attr.Value.Emit()
	}
	return result
}

func Test_Migrate(t *testing.T) {
	// A database of version 1, which had volumes, without how many files
	// were indexed, or what the last scan indexed, and not objects, gets
	// them, and keeps its volumes
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "index.db")
	db, err := ncruces.Open(ctx, path)
	require.NoError(t, err)
	require.NoError(t, db.Exec(ctx, `
		CREATE TABLE volume (
		  name TEXT PRIMARY KEY, url TEXT NOT NULL, enabled BOOLEAN NOT NULL,
		  created_at TIMESTAMP NOT NULL, modified_at TIMESTAMP, status TEXT,
		  folders INTEGER, index_started TIMESTAMP, index_timestamp TIMESTAMP, error TEXT
		);
		INSERT INTO volume (name, url, enabled, created_at, status, folders)
		VALUES ('media', 'file://media/mnt/media?index=folders', 1, '2026-11-19T12:00:00Z', 'ready', 3);
		PRAGMA user_version = 1;
	`))
	require.NoError(t, db.Close())

	s, err := sqlite.New(ctx, path)
	require.NoError(t, err)
	defer s.Close()
	_, err = s.GetVolume(ctx, "media")
	require.NoError(t, err)
	state, err := s.State(ctx, "media")
	require.NoError(t, err)
	assert.Zero(t, state.Folders, "its index is scanned again, as its objects are of keys")
	assert.Zero(t, state.Files)
	assert.Empty(t, state.Mode, "so it's scanned again before its files are listed from the index")
	state.Files, state.Mode = 7, "objects"
	require.NoError(t, s.SetState(ctx, "media", state))
	state, err = s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, uint64(7), state.Files)
	assert.Equal(t, "objects", state.Mode)
	require.NoError(t, s.PutObject(ctx, "media", &schema.Object{ObjectKey: schema.ObjectKey{Path: "/a"}, ObjectAttr: schema.ObjectAttr{IsDir: true}}))
	_, err = s.GetObject(ctx, "media", "/a/")
	assert.NoError(t, err)
	require.NoError(t, s.Push(ctx, "media", store.Work{Key: "/a/", Kind: store.KindIndex}))
}

func Test_MigrateKeys(t *testing.T) {
	// A database of version 5, whose objects, metadata, and work, were of
	// paths, has them removed, and its volumes' state, so they're scanned
	// again, and keeps its volumes
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "index.db")
	db, err := ncruces.Open(ctx, path)
	require.NoError(t, err)
	require.NoError(t, db.Exec(ctx, `
		CREATE TABLE volume (
		  name TEXT PRIMARY KEY, url TEXT NOT NULL, enabled BOOLEAN NOT NULL,
		  created_at TIMESTAMP NOT NULL, modified_at TIMESTAMP, status TEXT,
		  folders INTEGER, files INTEGER, scan_mode TEXT, index_started TIMESTAMP, index_timestamp TIMESTAMP, error TEXT
		);
		CREATE TABLE object (volume TEXT NOT NULL, path TEXT NOT NULL, key TEXT NOT NULL, parent TEXT NOT NULL, is_dir BOOLEAN NOT NULL,
		  type TEXT, size INTEGER, etag TEXT, modified_at TIMESTAMP, PRIMARY KEY (volume, path));
		CREATE TABLE metadata (volume TEXT NOT NULL, path TEXT NOT NULL, namespace TEXT, key TEXT NOT NULL, value TEXT NOT NULL, text TEXT);
		CREATE TABLE object_state (volume TEXT NOT NULL, path TEXT NOT NULL, kind TEXT NOT NULL, etag TEXT, after TIMESTAMP NOT NULL,
		  attempts INTEGER NOT NULL DEFAULT 0, leased_until TIMESTAMP, error TEXT, PRIMARY KEY (volume, path, kind));
		INSERT INTO volume (name, url, enabled, created_at, status, folders, files, scan_mode)
		VALUES ('media', 'file://media/mnt/media?index=objects', 1, '2026-11-19T12:00:00Z', 'ready', 3, 7, 'objects');
		INSERT INTO object (volume, path, key, parent, is_dir) VALUES ('media', '/a', '/a/', '/', 1);
		INSERT INTO object_state (volume, path, kind, after) VALUES ('media', '/a', 'index', '2026-11-19T12:00:00Z');
		PRAGMA user_version = 5;
	`))
	require.NoError(t, db.Close())

	s, err := sqlite.New(ctx, path)
	require.NoError(t, err)
	defer s.Close()
	_, err = s.GetVolume(ctx, "media")
	require.NoError(t, err)
	state, err := s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, schema.IndexState{}, state, "it's scanned again")
	objects, err := s.ListObjects(ctx, "media", store.ListRequest{Parent: "/", Recursive: true})
	require.NoError(t, err)
	assert.Empty(t, objects)
	work, err := s.Claim(ctx, "media", []store.Kind{store.KindIndex, store.KindRescan}, 10, time.Hour)
	require.NoError(t, err)
	assert.Empty(t, work)

	// Its objects are of keys, a file, and a folder, of the same path
	require.NoError(t, s.PutObject(ctx, "media",
		&schema.Object{ObjectKey: schema.ObjectKey{Path: "/a"}, ObjectAttr: schema.ObjectAttr{IsDir: true}},
		&schema.Object{ObjectKey: schema.ObjectKey{Path: "/a"}},
	))
	objects, err = s.ListObjects(ctx, "media", store.ListRequest{Parent: "/"})
	require.NoError(t, err)
	assert.Len(t, objects, 2)
}

func Test_MigrateRevisions(t *testing.T) {
	// A database of version 6, whose queue hasn't the revisions of files,
	// and whose objects haven't their media types, gets them, and keeps its
	// objects
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "index.db")
	db, err := ncruces.Open(ctx, path)
	require.NoError(t, err)
	require.NoError(t, db.Exec(ctx, `
		CREATE TABLE volume (
		  name TEXT PRIMARY KEY, url TEXT NOT NULL, enabled BOOLEAN NOT NULL,
		  created_at TIMESTAMP NOT NULL, modified_at TIMESTAMP, status TEXT,
		  folders INTEGER, files INTEGER, scan_mode TEXT, index_started TIMESTAMP, index_timestamp TIMESTAMP, error TEXT
		);
		CREATE TABLE object (
		  volume TEXT NOT NULL REFERENCES volume (name) ON DELETE CASCADE ON UPDATE CASCADE,
		  key TEXT NOT NULL CHECK (key GLOB '/*' AND key NOT GLOB '*//*'),
		  type TEXT, size INTEGER, etag TEXT, modified_at TIMESTAMP,
		  parent TEXT GENERATED ALWAYS AS (rtrim(rtrim(key, '/'), replace(rtrim(key, '/'), '/', ''))) VIRTUAL,
		  PRIMARY KEY (volume, key)
		);
		CREATE TABLE metadata (
		  volume TEXT NOT NULL, object TEXT NOT NULL, namespace TEXT, key TEXT NOT NULL, value TEXT NOT NULL, text TEXT,
		  FOREIGN KEY (volume, object) REFERENCES object (volume, key) ON DELETE CASCADE ON UPDATE CASCADE
		);
		CREATE UNIQUE INDEX metadata_key ON metadata (volume, object, ifnull(namespace, ''), key);
		CREATE TABLE object_state (
		  volume TEXT NOT NULL REFERENCES volume (name) ON DELETE CASCADE ON UPDATE CASCADE,
		  key TEXT NOT NULL, kind TEXT NOT NULL, etag TEXT, after TIMESTAMP NOT NULL,
		  attempts INTEGER NOT NULL DEFAULT 0, leased_until TIMESTAMP, error TEXT,
		  PRIMARY KEY (volume, key, kind)
		);
		INSERT INTO volume (name, url, enabled, created_at, status, folders, files, scan_mode)
		VALUES ('media', 'file://media/mnt/media?index=objects', 1, '2026-11-19T12:00:00Z', 'ready', 1, 1, 'objects');
		INSERT INTO object (volume, key) VALUES ('media', '/'), ('media', '/a.txt');
		INSERT INTO metadata (volume, object, namespace, key, value) VALUES ('media', '/a.txt', 'dc', 'title', '"A"');
		PRAGMA user_version = 6;
	`))
	require.NoError(t, db.Close())

	s, err := sqlite.New(ctx, path)
	require.NoError(t, err)
	defer s.Close()
	_, err = s.GetObject(ctx, "media", "/a.txt")
	require.NoError(t, err, "its objects are kept")

	// Its metadata is the backend's, which a task's is merged with
	object, err := s.GetObject(ctx, "media", "/a.txt")
	require.NoError(t, err)
	assert.Equal(t, []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"A"`)}}, object.Meta)
	require.NoError(t, s.SetMetadata(ctx, "media", "/a.txt", "metadata", nil, []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"B"`)}}, nil))
	object, err = s.GetObject(ctx, "media", "/a.txt")
	require.NoError(t, err)
	assert.Equal(t, []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"B"`)}}, object.Meta)

	revisions, err := s.Revisions(ctx, "media", "/", true)
	require.NoError(t, err)
	assert.Empty(t, revisions)
	require.NoError(t, s.SetRevisions(ctx, "media", map[string]string{"/a.txt": "1"}))
	revisions, err = s.Revisions(ctx, "media", "/", true)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"/a.txt": "1"}, revisions)
	require.NoError(t, s.PutObject(ctx, "media", &schema.Object{ObjectKey: schema.ObjectKey{Path: "/a.txt"}}))
	revisions, err = s.Revisions(ctx, "media", "/", true)
	require.NoError(t, err)
	assert.Empty(t, revisions, "its triggers are created")

	// Its objects have their media types, of a listing of a type
	require.NoError(t, s.PutObject(ctx, "media", &schema.Object{ObjectKey: schema.ObjectKey{Path: "/b.png"}, ObjectMeta: schema.ObjectMeta{ContentType: "image/png"}}))
	objects, err := s.ListObjects(ctx, "media", store.ListRequest{Parent: "/", Type: "image"})
	require.NoError(t, err)
	require.Len(t, objects, 1)
	assert.Equal(t, "/b.png", objects[0].Path)
}

func Test_PutRollback(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	_, err := s.CreateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/mnt/media?index=folders", Enabled: true})
	require.NoError(t, err)
	object := func(path, key, value string) *schema.Object {
		return &schema.Object{ObjectKey: schema.ObjectKey{Path: path}, ObjectMeta: schema.ObjectMeta{Meta: []schema.Meta{{Namespace: "dc", Key: key, Value: json.RawMessage(value)}}}}
	}
	require.NoError(t, s.PutObject(ctx, "media", object("/a", "title", `"before"`)))

	// A batch which updates an object, and its metadata, and then fails, on a
	// later object's metadata, whose key isn't an identifier, is rolled back,
	// as a whole
	err = s.PutObject(ctx, "media", object("/a", "title", `"after"`), object("/b", "title", `"b"`), object("/c", "not a key", `"c"`))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	a, err := s.GetObject(ctx, "media", "/a")
	require.NoError(t, err)
	require.Len(t, a.Meta, 1)
	assert.JSONEq(t, `"before"`, string(a.Meta[0].Value), "its update is rolled back")
	for _, path := range []string{"/b", "/c"} {
		_, err := s.GetObject(ctx, "media", path)
		assert.ErrorIs(t, err, gofiler.ErrNotFound, path)
	}
}
