package sqlite

import (
	"context"
	"errors"
	"slices"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	schema "github.com/mutablelogic/go-filer/store/sqlite/schema"
	sqlite "github.com/mutablelogic/go-sqlite"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// SetObjectArtwork stores the artwork, or gets the artwork of each one's
// hash, which is stored already, and links the object of the volume of the
// key to it, replacing its links, in one transaction, and returns it,
// without its data, in the order of its contexts
func (self *Store) SetObjectArtwork(ctx context.Context, volume, key string, artwork ...filer.ArtworkMeta) (_ []*filer.Artwork, err error) {
	ctx, end := self.span(ctx, "SetObjectArtwork", attribute.String("arg.volume", volume), attribute.String("arg.key", key), attribute.Int("arg.artwork", len(artwork)))
	defer func() { end(err) }()
	if err := store.ValidateKey(key); err != nil {
		return nil, err
	} else if artwork, err = normalizeArtwork(artwork); err != nil {
		return nil, err
	}
	var result []*filer.Artwork
	if err := self.conn.Tx(ctx, func(conn sqlite.Conn) error {
		var keys schema.Keys
		if err := conn.Get(ctx, &keys, schema.ObjectArtwork{Volume: volume, Key: key}); errors.Is(err, sqlite.ErrNotFound) {
			return gofiler.ErrNotFound.Withf("object %q of volume %q", key, volume)
		} else if err != nil {
			return err
		}
		result, err = linkArtwork(ctx, conn, volume, key, artwork)
		return err
	}); err != nil {
		return nil, storeErr(err)
	}
	return result, nil
}

// GetArtwork returns the artwork of the hash, with its data, when it's
// asked for, or ErrNotFound
func (self *Store) GetArtwork(ctx context.Context, hash string, data bool) (_ *filer.Artwork, err error) {
	ctx, end := self.span(ctx, "GetArtwork", attribute.String("arg.hash", hash), attribute.Bool("arg.data", data))
	defer func() { end(err) }()
	hash, err = store.ArtworkHash(hash)
	if err != nil {
		return nil, err
	}
	var result schema.ArtworkList
	if err := self.conn.Get(ctx, &result, schema.ArtworkHash{Hash: hash, Data: data}); errors.Is(err, sqlite.ErrNotFound) {
		return nil, gofiler.ErrNotFound.Withf("artwork %s", hash)
	} else if err != nil {
		return nil, storeErr(err)
	}
	return result[0], nil
}

// ObjectArtwork returns the artwork the object of the volume of the key
// links to, without its data, in the order of its contexts
func (self *Store) ObjectArtwork(ctx context.Context, volume, key string) (_ []*filer.Artwork, err error) {
	ctx, end := self.span(ctx, "ObjectArtwork", attribute.String("arg.volume", volume), attribute.String("arg.key", key))
	defer func() { end(err) }()
	result := schema.ArtworkList{}
	if err := self.conn.List(ctx, &result, schema.ObjectArtwork{Volume: volume, Key: key}); err != nil {
		return nil, storeErr(err)
	}
	return result, nil
}

// UnlinkVolumeArtwork removes the links of the objects of the volume to
// their artwork, but not what tasks found of them, otherwise
func (self *Store) UnlinkVolumeArtwork(ctx context.Context, volume string) (err error) {
	ctx, end := self.span(ctx, "UnlinkVolumeArtwork", attribute.String("arg.volume", volume))
	defer func() { end(err) }()
	if err := self.conn.Delete(ctx, nil, schema.VolumeArtwork(volume)); err != nil && !errors.Is(err, sqlite.ErrNotFound) {
		return storeErr(err)
	}
	return nil
}

// DeleteUnlinkedArtwork deletes the artwork which no object links to, and
// returns how many were deleted
func (self *Store) DeleteUnlinkedArtwork(ctx context.Context) (_ int, err error) {
	ctx, end := self.span(ctx, "DeleteUnlinkedArtwork")
	defer func() { end(err) }()
	var deleted schema.ArtworkList
	if err := self.conn.Delete(ctx, &deleted, schema.ArtworkUnlinked{}); errors.Is(err, sqlite.ErrNotFound) {
		return 0, nil
	} else if err != nil {
		return 0, storeErr(err)
	}
	return len(deleted), nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// normalizeArtwork returns the artwork, of normalized types, or
// ErrBadParameter when one isn't valid, or two are of the same context
func normalizeArtwork(artwork []filer.ArtworkMeta) ([]filer.ArtworkMeta, error) {
	result := make([]filer.ArtworkMeta, len(artwork))
	contexts := make(map[string]bool, len(artwork))
	for i, a := range artwork {
		var err error
		if result[i], err = store.NormalizeArtwork(a); err != nil {
			return nil, err
		} else if contexts[a.Context] {
			return nil, gofiler.ErrBadParameter.Withf("more than one artwork is of context %q", a.Context)
		}
		contexts[a.Context] = true
	}
	return result, nil
}

// linkArtwork replaces the links of the object of the volume of the key, of
// a transaction, with the artwork, which is normalized, and is stored, or
// is the artwork of its hash, which is stored already, of its type and
// size, and returns it, without its data, in the order of its contexts
func linkArtwork(ctx context.Context, conn sqlite.Conn, volume, key string, artwork []filer.ArtworkMeta) ([]*filer.Artwork, error) {
	var deleted schema.Keys
	if err := conn.Delete(ctx, &deleted, schema.ObjectArtwork{Volume: volume, Key: key}); err != nil && !errors.Is(err, sqlite.ErrNotFound) {
		return nil, err
	}
	result := make([]*filer.Artwork, 0, len(artwork))
	for _, meta := range artwork {
		var stored schema.ArtworkList
		if err := conn.Insert(ctx, &stored, schema.ArtworkPut(meta)); err != nil {
			return nil, err
		} else if a := stored[0]; a.Type != meta.Type || a.Width != meta.Width || a.Height != meta.Height {
			return nil, gofiler.ErrConflict.Withf("artwork %s is stored as %q, %dx%d, not %q, %dx%d", a.Hash, a.Type, a.Width, a.Height, meta.Type, meta.Width, meta.Height)
		}
		var inserted schema.Keys
		if err := conn.Insert(ctx, &inserted, schema.ObjectArtworkLink{Volume: volume, Key: key, Hash: stored[0].Hash}); err != nil {
			return nil, err
		}
		result = append(result, stored[0])
	}
	slices.SortFunc(result, func(a, b *filer.Artwork) int {
		if c := strings.Compare(a.Context, b.Context); c != 0 {
			return c
		}
		return strings.Compare(a.Hash, b.Hash)
	})
	return result, nil
}
