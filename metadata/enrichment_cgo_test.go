//go:build cgo

package metadata_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fs "github.com/mutablelogic/go-filer/backend/fs"
	metadata "github.com/mutablelogic/go-filer/metadata"
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_EnrichMedia(t *testing.T) {
	// With cgo, a photo's EXIF metadata is read, with libexif
	dir := t.TempDir()
	data, err := os.ReadFile(filepath.Join("testdata", "canon.jpg"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "canon.jpg"), data, 0o644))
	backend, err := fs.New("media", dir, nil, gofiler.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	e, err := metadata.New()
	require.NoError(t, err)
	object, err := backend.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/canon.jpg"}})
	require.NoError(t, err)
	store := metadataStore{}
	require.NoError(t, e.Enrich(context.Background(), backend, store, object))
	namespaces := map[string]bool{}
	for _, m := range store["/canon.jpg"] {
		namespaces[m.Namespace] = true
	}
	assert.True(t, namespaces["exif"], "its EXIF metadata, of %v", namespaces)
}
