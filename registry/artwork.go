package registry

import (
	"context"
	"errors"
	"log/slog"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	// artworkInterval is how often the artwork which no object links to is
	// deleted, such as of objects which changed, or were deleted, and
	// artworkGrace how long ago it was last put, so artwork which is put,
	// and is about to be linked to, isn't
	artworkInterval = time.Hour
	artworkGrace    = time.Hour
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// cleanArtwork deletes the artwork which no object links to, every
// artworkInterval, until the context is cancelled, or the store doesn't
// keep artwork
func (self *Registry) cleanArtwork(ctx context.Context, logger *slog.Logger) {
	ticker := time.NewTicker(artworkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !self.deleteUnlinkedArtwork(ctx, logger) {
				return
			}
		}
	}
}

// deleteUnlinkedArtwork deletes the artwork which no object links to, which
// was last put before artworkGrace, and logs how many were deleted, and
// returns false when the store doesn't keep artwork
func (self *Registry) deleteUnlinkedArtwork(ctx context.Context, logger *slog.Logger) bool {
	n, err := self.store.DeleteUnlinkedArtwork(ctx, time.Now().Add(-artworkGrace))
	switch {
	case errors.Is(err, gofiler.ErrNotImplemented):
		logger.DebugContext(ctx, "the store doesn't keep artwork, so it isn't cleaned up")
		return false
	case err != nil && ctx.Err() == nil:
		logger.WarnContext(ctx, "artwork which no object links to can't be deleted", "error", err.Error())
	case n > 0:
		logger.InfoContext(ctx, "deleted artwork which no object links to", "artwork", n)
	}
	return true
}
