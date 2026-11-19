package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	store "github.com/mutablelogic/go-filer/filer/store"
	sqlite "github.com/mutablelogic/go-filer/filer/store/sqlite"
	storetest "github.com/mutablelogic/go-filer/filer/store/storetest"
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
	_, err = s.PutVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/mnt/media?index=folders", Enabled: true})
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
	_, err = s.PutVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/mnt/media?index=none", Enabled: true})
	require.NoError(t, err)
	_, err = s.GetVolume(ctx, "missing")
	require.ErrorIs(t, err, gofiler.ErrNotFound)

	spans := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range recorder.Ended() {
		spans[span.Name()] = span
	}
	for _, name := range []string{"filer.store.sqlite.New", "filer.store.sqlite.PutVolume", "filer.store.sqlite.GetVolume"} {
		require.Contains(t, spans, name)
	}
	put := spans["filer.store.sqlite.PutVolume"]
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
	// A database of version 1, which had volumes, but not objects, gets
	// them, and keeps its volumes
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "index.db")
	s, err := sqlite.New(ctx, path)
	require.NoError(t, err)
	_, err = s.PutVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/mnt/media?index=folders", Enabled: true})
	require.NoError(t, err)
	require.NoError(t, s.Close())
	db, err := ncruces.Open(ctx, path)
	require.NoError(t, err)
	require.NoError(t, db.Exec(ctx, `DROP TABLE metadata; DROP TABLE object; PRAGMA user_version = 1`))
	require.NoError(t, db.Close())

	s, err = sqlite.New(ctx, path)
	require.NoError(t, err)
	defer s.Close()
	_, err = s.GetVolume(ctx, "media")
	require.NoError(t, err)
	require.NoError(t, s.Put(ctx, "media", &schema.Object{ObjectKey: schema.ObjectKey{Path: "/a"}, ObjectAttr: schema.ObjectAttr{IsDir: true}}))
	_, err = s.Get(ctx, "media", "/a")
	assert.NoError(t, err)
}
