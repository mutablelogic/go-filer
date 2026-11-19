package enrichment_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fs "github.com/mutablelogic/go-filer/backend/fs"
	enrichment "github.com/mutablelogic/go-filer/enrichment"
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// metadataStore is a task's store, which keeps the metadata it's set, of
// each key
type metadataStore map[string][]schema.Meta

func (m metadataStore) SetMetadata(_ context.Context, key string, meta []schema.Meta) error {
	m[key] = meta
	return nil
}

// value returns the value of the metadata of the namespace and key
func value(meta []schema.Meta, ns, key string) string {
	for _, m := range meta {
		if m.Namespace == ns && m.Key == key {
			return string(m.Value)
		}
	}
	return ""
}

func Test_Enrich(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "page.html"), []byte(`<html><head><title>A Page</title><meta name="description" content="About it"></head><body>Hello</body></html>`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Holiday Plans.xyz"), []byte("?"), 0o644))
	backend, err := fs.New("media", dir, nil, gofiler.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	e, err := enrichment.New()
	require.NoError(t, err)
	enrich := func(path string) []schema.Meta {
		t.Helper()
		object, err := backend.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: path}})
		require.NoError(t, err)
		store := metadataStore{}
		require.NoError(t, e.Enrich(context.Background(), backend, store, object))
		return store[path]
	}

	// A file's metadata is go-metadata's, of its type, and not its content
	meta := enrich("/page.html")
	assert.Equal(t, `"A Page"`, value(meta, "dc", "title"))
	assert.Equal(t, `"About it"`, value(meta, "dc", "description"))
	assert.Empty(t, value(meta, "markdown", "body"), "its content isn't its metadata")

	// One whose type no handler reads has the metadata of its name
	meta = enrich("/Holiday Plans.xyz")
	assert.Equal(t, `"Holiday Plans"`, value(meta, "dc", "title"))
}

func Test_New(t *testing.T) {
	// The keys of TMDB, and AcoustID, can't be empty
	_, err := enrichment.New(enrichment.WithTMDB(""))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = enrichment.New(enrichment.WithAcoustID(""))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = enrichment.New(enrichment.WithTMDB("key"), enrichment.WithAcoustID("key"), enrichment.WithMaxSize(1<<20))
	assert.NoError(t, err)
}
