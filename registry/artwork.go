package registry

import (
	"context"
	"errors"
	"log/slog"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	// artworkInterval is how often the artwork which no object links to is
	// deleted, such as of objects which changed, or were deleted
	artworkInterval = time.Hour
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// GetArtwork returns the artwork of the hash, which objects link to (see
// schema.Object's Artwork), with its data, when it's asked for, or
// ErrNotFound, or ErrBadParameter when the hash isn't one
func (self *Registry) GetArtwork(ctx context.Context, hash string, data bool) (*schema.Artwork, error) {
	return self.store.GetArtwork(ctx, hash, data)
}

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

// deleteUnlinkedArtwork deletes the artwork which no object links to, and
// logs how many were deleted, and returns false when the store doesn't keep
// artwork
func (self *Registry) deleteUnlinkedArtwork(ctx context.Context, logger *slog.Logger) bool {
	n, err := self.store.DeleteUnlinkedArtwork(ctx)
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
