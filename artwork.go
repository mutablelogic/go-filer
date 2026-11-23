package filer

import (
	"context"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ArtworkStore keeps artwork: images, such as an album's cover, or a
// thumbnail, of their type, size and context, by the hash of their context
// and data, which objects of volumes' indexes link to, so artwork is kept
// once, however many objects link to it, and an object links to artwork of
// one context once, at most. Artwork is stored as it's linked to, in one
// transaction, so artwork is never stored, but not linked to, meanwhile.
type ArtworkStore interface {
	// SetObjectArtwork stores the artwork, or gets the artwork of each one's
	// hash, which is stored already, and links the object of the volume of
	// the key to it, replacing its links, in one transaction, and returns
	// it, without its data, in the order of its contexts, or removes its
	// links, with none. Returns ErrNotFound when the object isn't stored,
	// ErrBadParameter when artwork isn't valid, or two are of the same
	// context, and ErrConflict when the artwork of a hash, which is stored
	// already, has another type, or size. An object's links are removed
	// when it's deleted, or changes, of its entity tag, size or
	// modification time, as a task's metadata of it is.
	SetObjectArtwork(ctx context.Context, volume, key string, artwork ...schema.ArtworkMeta) ([]*schema.Artwork, error)

	// GetArtwork returns the artwork of the hash, with its data, when it's
	// asked for, or ErrNotFound
	GetArtwork(ctx context.Context, hash string, data bool) (*schema.Artwork, error)

	// ObjectArtwork returns the artwork the object of the volume of the key
	// links to, without its data, in the order of its contexts, which is
	// none when it links to none, or isn't stored
	ObjectArtwork(ctx context.Context, volume, key string) ([]*schema.Artwork, error)

	// UnlinkVolumeArtwork removes the links of the objects of the volume to
	// their artwork, but not what tasks found of them, otherwise, such as
	// when its files aren't enriched with artwork any more, whose artwork
	// is deleted once no object links to it (see DeleteUnlinkedArtwork)
	UnlinkVolumeArtwork(ctx context.Context, volume string) error

	// DeleteUnlinkedArtwork deletes the artwork which no object links to,
	// such as of objects which changed, or were deleted, and returns how
	// many were deleted
	DeleteUnlinkedArtwork(ctx context.Context) (int, error)
}
