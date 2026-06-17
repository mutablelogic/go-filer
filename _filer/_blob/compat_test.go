package blob

import (
	"bytes"
	"context"
	"io"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	blob "gocloud.dev/blob"
)

// TestLegacyLeadingSlashKeyCompatibility verifies reads/lists/deletes can find
// historical S3-style keys that were written with a leading slash.
func TestLegacyLeadingSlashKeyCompatibility(t *testing.T) {
	ctx := context.Background()

	backend, err := New(ctx, "mem://testbucket")
	require.NoError(t, err)
	defer backend.Close()

	const key = "/NPR6471846591.mp3"
	const content = "legacy-key-content"

	w, err := backend.bucket.NewWriter(ctx, key, &blob.WriterOptions{ContentType: "audio/mpeg"})
	require.NoError(t, err)
	_, err = io.Copy(w, bytes.NewBufferString(content))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	obj, err := backend.GetObject(ctx, schema.GetObjectRequest{Path: "/NPR6471846591.mp3"})
	require.NoError(t, err)
	assert.Equal(t, "/NPR6471846591.mp3", obj.Path)

	listResp, err := backend.ListObjects(ctx, schema.ListObjectsRequest{
		Path:  "/NPR6471846591.mp3",
		Limit: schema.MaxListLimit,
	})
	require.NoError(t, err)
	require.Len(t, listResp.Body, 1)
	assert.Equal(t, "/NPR6471846591.mp3", listResp.Body[0].Path)

	r, _, err := backend.ReadObject(ctx, schema.ReadObjectRequest{GetObjectRequest: schema.GetObjectRequest{Path: "/NPR6471846591.mp3"}})
	require.NoError(t, err)
	defer r.Close()
	body, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, content, string(body))

	deleted, err := backend.DeleteObject(ctx, schema.DeleteObjectRequest{Path: "/NPR6471846591.mp3"})
	require.NoError(t, err)
	assert.Equal(t, "/NPR6471846591.mp3", deleted.Path)

	_, err = backend.GetObject(ctx, schema.GetObjectRequest{Path: "/NPR6471846591.mp3"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
