package registry

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// unkeptArtwork is a store which doesn't keep artwork
type unkeptArtwork struct {
	store.Store
}

func (unkeptArtwork) DeleteUnlinkedArtwork(context.Context, time.Time) (int, error) {
	return 0, gofiler.ErrNotImplemented.With("artwork")
}

// Test_CleanArtwork tests the artwork which no object links to is deleted,
// once it was put a while ago, by the registry, as it runs
func Test_CleanArtwork(t *testing.T) {
	interval, grace := artworkInterval, artworkGrace
	artworkInterval, artworkGrace = 10*time.Millisecond, 0
	t.Cleanup(func() { artworkInterval, artworkGrace = interval, grace })

	s := newStore(t)
	artwork, err := s.PutArtwork(context.Background(), schema.ArtworkMeta{Type: "image/png", Width: 1, Height: 1, Context: schema.ArtworkCover, Data: []byte("png")})
	require.NoError(t, err)
	r, err := New(context.Background(), WithDebounce(0), WithStore(s))
	require.NoError(t, err)
	t.Cleanup(func() { r.close() })
	runRegistry(t, r)
	require.Eventually(t, func() bool {
		_, err := s.GetArtwork(context.Background(), artwork.Hash, false)
		return err != nil
	}, 5*time.Second, 10*time.Millisecond)
	_, err = s.GetArtwork(context.Background(), artwork.Hash, false)
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
}

// Test_CleanArtworkNotKept tests a store which doesn't keep artwork isn't
// asked again
func Test_CleanArtworkNotKept(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := &Registry{store: unkeptArtwork{newStore(t)}}
	assert.False(t, r.deleteUnlinkedArtwork(context.Background(), logger))
	assert.Contains(t, buf.String(), "doesn't keep artwork")
}
