package filer

import (
	"context"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ArtworkStore keeps artwork: images, such as an album's cover, or a
// thumbnail, of their type, size and context, by the hash of their context
// and data, which objects of volumes' indexes link to, so artwork is kept
// once, however many objects link to it, and an object links to artwork of
// one context once, at most.
type ArtworkStore interface {
	// PutArtwork stores artwork, and returns it, without its data, or the
	// artwork of its hash, which is stored already. Returns ErrBadParameter
	// when it isn't valid, and ErrConflict when the artwork of its hash
	// has another type, or size.
	PutArtwork(ctx context.Context, artwork schema.ArtworkMeta) (*schema.Artwork, error)

	// GetArtwork returns the artwork of the hash, with its data, when it's
	// asked for, or ErrNotFound
	GetArtwork(ctx context.Context, hash string, data bool) (*schema.Artwork, error)

	// SetObjectArtwork links the object of the volume of the key to the
	// artwork of the hashes, replacing its links, or removes them, with
	// none. Returns ErrNotFound when the object, or artwork, isn't stored,
	// and ErrBadParameter when two are of the same context. An object's
	// links are removed when it's deleted, or changes, of its entity tag,
	// size or modification time, as a task's metadata of it is.
	SetObjectArtwork(ctx context.Context, volume, key string, hashes ...string) error

	// ObjectArtwork returns the artwork the object of the volume of the key
	// links to, without its data, in the order of its contexts, which is
	// none when it links to none, or isn't stored
	ObjectArtwork(ctx context.Context, volume, key string) ([]*schema.Artwork, error)

	// DeleteUnlinkedArtwork deletes the artwork which no object links to,
	// which was last put before the time, so artwork which is put, and is
	// about to be linked to, isn't, and returns how many were deleted
	DeleteUnlinkedArtwork(ctx context.Context, before time.Time) (int, error)
}
