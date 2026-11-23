package metadata_test

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"testing"
	"testing/iotest"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fs "github.com/mutablelogic/go-filer/backend/fs"
	metadata "github.com/mutablelogic/go-filer/metadata"
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// metadataStore is a task's store, which keeps the metadata it's set, of
// each key
type metadataStore map[string][]schema.Meta

func (m metadataStore) SetMetadata(_ context.Context, key string, found schema.TaskMeta) error {
	meta := found.Meta
	m[key] = meta
	return nil
}

// Artwork returns false, as the volume isn't enriched with artwork
func (metadataStore) Artwork() bool {
	return false
}

// artworkStore is a task's store, of a volume which is enriched with its
// artwork, which keeps what it's set, of each key
type artworkStore map[string]schema.TaskMeta

func (m artworkStore) SetMetadata(_ context.Context, key string, found schema.TaskMeta) error {
	m[key] = found
	return nil
}

func (artworkStore) Artwork() bool {
	return true
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
	e, err := metadata.New()
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
	_, err := metadata.New(metadata.WithTMDB(""))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = metadata.New(metadata.WithAcoustID(""))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = metadata.New(metadata.WithTMDB("key"), metadata.WithAcoustID("key"), metadata.WithMaxSize(1<<20))
	assert.NoError(t, err)
}

// failing is a backend whose files fail to be read, once they're opened
type failing struct {
	gofiler.Filer
}

func (f failing) ReadObject(ctx context.Context, req schema.GetObjectRequest) (gofiler.ReadObjectFunc, *schema.Object, error) {
	_, object, err := f.Filer.ReadObject(ctx, req)
	if err != nil {
		return nil, nil, err
	}
	return func(ctx context.Context, fn func(io.Reader) error) error {
		return fn(iotest.ErrReader(errors.New("it can't be read")))
	}, object, nil
}

func Test_EnrichFails(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Holiday Plans.html"), []byte("<html><title>A</title></html>"), 0o644))
	backend, err := fs.New("media", dir, nil, gofiler.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	e, err := metadata.New()
	require.NoError(t, err)
	object, err := backend.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/Holiday Plans.html"}})
	require.NoError(t, err)

	// A file which fails to be read is an error, rather than enriched, of
	// what was extracted of it, so it's enriched again
	store := metadataStore{}
	err = e.Enrich(context.Background(), failing{backend}, store, object)
	assert.ErrorContains(t, err, "it can't be read")
}

func Test_EnrichUnreadable(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cut short.pdf"), []byte("%PDF-1.7\n1 0 obj\n<< /Type /Catalog"), 0o644))
	backend, err := fs.New("media", dir, nil, gofiler.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	e, err := metadata.New()
	require.NoError(t, err)
	object, err := backend.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/cut short.pdf"}})
	require.NoError(t, err)

	// A file which go-metadata can't read, as it isn't valid, is go-filer's
	// ErrBadParameter, of go-metadata's message, so it's given up at once
	err = e.Enrich(context.Background(), backend, metadataStore{}, object)
	require.Error(t, err)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	assert.NotContains(t, err.Error(), "bad parameter: bad parameter")
}

// Test_EnrichArtwork tests a file's artwork is found, of the same read as its
// metadata, when its volume is enriched with artwork, and not otherwise
func Test_EnrichArtwork(t *testing.T) {
	dir := t.TempDir()
	img := image.NewRGBA(image.Rect(0, 0, 40, 20))
	for x := range 40 {
		img.Set(x, x%20, color.RGBA{R: 255, A: 255})
	}
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, img))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "red.png"), buf.Bytes(), 0o644))
	backend, err := fs.New("media", dir, nil, gofiler.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	e, err := metadata.New()
	require.NoError(t, err)
	object, err := backend.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/red.png"}})
	require.NoError(t, err)

	// Its artwork, of its type, size, context and data, as well as its
	// metadata, which hasn't the artwork
	art := artworkStore{}
	require.NoError(t, e.Enrich(context.Background(), backend, art, object))
	found := art["/red.png"]
	require.NotEmpty(t, found.Artwork)
	a := found.Artwork[0]
	assert.Equal(t, "image/png", a.Type)
	assert.Equal(t, uint(40), a.Width)
	assert.Equal(t, uint(20), a.Height)
	assert.NotEmpty(t, a.Context)
	assert.NotEmpty(t, a.Data)
	assert.Equal(t, `"red"`, value(found.Meta, "dc", "title"), "its metadata, of the same read")
	for _, m := range found.Meta {
		assert.NotEqual(t, "artwork", m.Namespace, "artwork isn't metadata")
	}

	// A volume which isn't enriched with artwork hasn't any
	plain := metadataStore{}
	require.NoError(t, e.Enrich(context.Background(), backend, plain, object))
	assert.Equal(t, `"red"`, value(plain["/red.png"], "dc", "title"))
}
