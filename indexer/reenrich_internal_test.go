package indexer

import (
	"context"
	"encoding/json"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fs "github.com/mutablelogic/go-filer/backend/fs"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// Test_Reenrich tests a volume whose files were enriched of the other side
// of ModeArtwork, of its last scan, is enriched again, of every file, once,
// and not otherwise
func Test_Reenrich(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.New(ctx, gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	backend, err := fs.New("media", t.TempDir(), nil, gofiler.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	_, err = s.CreateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/tmp", Enabled: true})
	require.NoError(t, err)
	size := int64(1)
	require.NoError(t, s.PutObject(ctx, "media", &schema.Object{ObjectKey: schema.ObjectKey{Path: "/"}, ObjectAttr: schema.ObjectAttr{IsDir: true}}, &schema.Object{ObjectKey: schema.ObjectKey{Path: "/a.png"}, ObjectAttr: schema.ObjectAttr{Size: &size}}))

	// enriched sets what the task found of the file, and the mode of the
	// volume's last scan, and returns whether it's kept, once an indexer of
	// the mode is run
	enriched := func(last, mode Mode) bool {
		t.Helper()
		require.NoError(t, s.SetMetadata(ctx, "media", "/a.png", string(store.KindMetadata), nil, schema.TaskMeta{
			Meta:    []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"A"`)}},
			Artwork: []schema.ArtworkMeta{{Type: "image/png", Width: 1, Height: 1, Context: schema.ArtworkThumbnail, Data: []byte("png")}},
		}))
		require.NoError(t, s.SetState(ctx, "media", schema.IndexState{Status: schema.IndexStatusReady, Mode: string(last)}))
		i, err := New("media", backend, s, WithMode(mode), WithMetadataTask(func(context.Context, gofiler.Filer, MetadataStore, *schema.Object) error { return nil }))
		require.NoError(t, err)
		i.reenrich(ctx)
		object, err := s.GetObject(ctx, "media", "/a.png")
		require.NoError(t, err)
		artwork, err := s.ObjectArtwork(ctx, "media", "/a.png")
		require.NoError(t, err)
		state, err := s.State(ctx, "media")
		require.NoError(t, err)
		kept := len(object.Meta) > 0
		assert.Equal(t, kept, len(artwork) > 0, "what the task found is kept, or not, with its artwork")
		if kept {
			assert.Equal(t, string(last), state.Mode, "its state's mode is its last scan's")
		} else {
			assert.Equal(t, string(mode), state.Mode, "its state's mode is the indexer's, so it isn't enriched again")
		}
		return kept
	}

	// Of the other side of ModeArtwork, it's enriched again, and not of the
	// same side, or of a volume which wasn't enriched, or scanned
	assert.False(t, enriched(ModeMetadata, ModeArtwork))
	assert.False(t, enriched(ModeArtwork, ModeMetadata))
	assert.True(t, enriched(ModeArtwork, ModeArtwork))
	assert.True(t, enriched(ModeMetadata, ModeMetadata))
	assert.True(t, enriched(ModeObjects, ModeArtwork), "it wasn't enriched")
}
