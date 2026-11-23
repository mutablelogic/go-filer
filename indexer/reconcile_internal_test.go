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

// Test_Reconcile tests a volume whose files were enriched with something
// else than its mode says, of what its state records, is enriched again, of
// every file, once, and not otherwise, whatever its last scan's mode
func Test_Reconcile(t *testing.T) {
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

	// enriched sets what the task found of the file, and what the volume's
	// state says it was enriched with, of a scan which is in progress, so its
	// mode is empty, and returns whether its metadata, and its links to
	// artwork, are kept, once the indexer of the mode reconciles it
	enriched := func(was string, mode Mode) (bool, bool) {
		t.Helper()
		require.NoError(t, s.SetMetadata(ctx, "media", "/a.png", string(store.KindMetadata), nil, schema.TaskMeta{
			Meta:    []schema.Meta{{Namespace: "dc", Key: "title", Value: json.RawMessage(`"A"`)}},
			Artwork: []schema.ArtworkMeta{{Type: "image/png", Width: 1, Height: 1, Context: schema.ArtworkThumbnail, Data: []byte("png")}},
		}))
		require.NoError(t, s.SetState(ctx, "media", schema.IndexState{Status: schema.IndexStatusScanning, Enriched: was}))
		i, err := New("media", backend, s, WithMode(mode), WithMetadataTask(func(context.Context, gofiler.Filer, MetadataStore, *schema.Object) error { return nil }))
		require.NoError(t, err)
		require.NoError(t, i.reconcile(ctx))
		object, err := s.GetObject(ctx, "media", "/a.png")
		require.NoError(t, err)
		artwork, err := s.ObjectArtwork(ctx, "media", "/a.png")
		require.NoError(t, err)
		state, err := s.State(ctx, "media")
		require.NoError(t, err)
		assert.Equal(t, i.enrichment(), state.Enriched, "its state records what its files are enriched with, so it isn't enriched again")
		return len(object.Meta) > 0, len(artwork) > 0
	}

	// Of something else, it's enriched again, and not of the same, or of
	// nothing recorded, which has nothing of it, and of artwork, to
	// metadata, only its links to artwork are removed, as its metadata is
	// the same
	for _, test := range []struct {
		was           string
		mode          Mode
		meta, artwork bool
	}{
		{"metadata", ModeArtwork, false, false},
		{"artwork", ModeMetadata, true, false},
		{"artwork", ModeObjects, false, false},
		{"metadata", ModeObjects, false, false},
		{"artwork", ModeArtwork, true, true},
		{"metadata", ModeMetadata, true, true},
		{"", ModeArtwork, true, true},
	} {
		meta, artwork := enriched(test.was, test.mode)
		assert.Equal(t, test.meta, meta, "its metadata, of %q, to %v", test.was, test.mode)
		assert.Equal(t, test.artwork, artwork, "its links to artwork, of %q, to %v", test.was, test.mode)
	}
}

// Test_ReconcileFails tests what a volume's files were enriched with isn't
// recorded when what was found of them can't be removed, so it's tried again
func Test_ReconcileFails(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.New(ctx, gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	backend, err := fs.New("media", t.TempDir(), nil, gofiler.Opts{})
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	_, err = s.CreateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/tmp", Enabled: true})
	require.NoError(t, err)
	require.NoError(t, s.SetState(ctx, "media", schema.IndexState{Enriched: "metadata"}))
	i, err := New("media", backend, failingStore{s}, WithMode(ModeArtwork), WithMetadataTask(func(context.Context, gofiler.Filer, MetadataStore, *schema.Object) error { return nil }))
	require.NoError(t, err)
	assert.ErrorIs(t, i.reconcile(ctx), gofiler.ErrInternalServerError)
	state, err := s.State(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, "metadata", state.Enriched)
}

// failingStore is a store whose task results can't be removed
type failingStore struct {
	store.Store
}

func (failingStore) DeleteMetadata(context.Context, string, string) error {
	return gofiler.ErrInternalServerError.With("it failed")
}
