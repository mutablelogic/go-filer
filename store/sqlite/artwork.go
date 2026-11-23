package sqlite

import (
	"context"
	"errors"
	"time"

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

// PutArtwork stores artwork, and returns it, without its data, or the
// artwork of its hash, which is stored already, or ErrConflict when that
// has another type, or size
func (self *Store) PutArtwork(ctx context.Context, artwork filer.ArtworkMeta) (_ *filer.Artwork, err error) {
	ctx, end := self.span(ctx, "PutArtwork", attribute.String("arg.artwork", artwork.String()))
	defer func() { end(err) }()
	if artwork, err = store.NormalizeArtwork(artwork); err != nil {
		return nil, err
	}
	var result schema.ArtworkList
	if err := self.conn.Insert(ctx, &result, schema.ArtworkPut(artwork)); err != nil {
		return nil, storeErr(err)
	}
	stored := result[0]
	if stored.Type != artwork.Type || stored.Width != artwork.Width || stored.Height != artwork.Height {
		return nil, gofiler.ErrConflict.Withf("artwork %s is stored as %q, %dx%d, not %q, %dx%d", stored.Hash, stored.Type, stored.Width, stored.Height, artwork.Type, artwork.Width, artwork.Height)
	}
	return stored, nil
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

// SetObjectArtwork links the object of the volume of the key to the artwork
// of the hashes, replacing its links, or removes them, with none, or
// returns ErrNotFound when the object, or artwork, isn't stored, or
// ErrBadParameter when two are of the same context
func (self *Store) SetObjectArtwork(ctx context.Context, volume, key string, hashes ...string) (err error) {
	ctx, end := self.span(ctx, "SetObjectArtwork", attribute.String("arg.volume", volume), attribute.String("arg.key", key), attribute.StringSlice("arg.hashes", hashes))
	defer func() { end(err) }()
	if err := store.ValidateKey(key); err != nil {
		return err
	}
	for i, hash := range hashes {
		if hashes[i], err = store.ArtworkHash(hash); err != nil {
			return err
		}
	}
	return storeErr(self.conn.Tx(ctx, func(conn sqlite.Conn) error {
		var keys schema.Keys
		if err := conn.Get(ctx, &keys, schema.ObjectArtwork{Volume: volume, Key: key}); errors.Is(err, sqlite.ErrNotFound) {
			return gofiler.ErrNotFound.Withf("object %q of volume %q", key, volume)
		} else if err != nil {
			return err
		}

		// The artwork, which is stored, of one context, once
		contexts := make(map[string]string, len(hashes))
		for _, hash := range hashes {
			var artwork schema.ArtworkList
			if err := conn.Get(ctx, &artwork, schema.ArtworkHash{Hash: hash}); errors.Is(err, sqlite.ErrNotFound) {
				return gofiler.ErrNotFound.Withf("artwork %s", hash)
			} else if err != nil {
				return err
			}
			if other, exists := contexts[artwork[0].Context]; exists && other != hash {
				return gofiler.ErrBadParameter.Withf("artwork %s and %s are both of context %q", other, hash, artwork[0].Context)
			}
			contexts[artwork[0].Context] = hash
		}

		// Its links are replaced
		var deleted schema.Keys
		if err := conn.Delete(ctx, &deleted, schema.ObjectArtwork{Volume: volume, Key: key}); err != nil && !errors.Is(err, sqlite.ErrNotFound) {
			return err
		}
		for _, hash := range contexts {
			var inserted schema.Keys
			if err := conn.Insert(ctx, &inserted, schema.ObjectArtworkLink{Volume: volume, Key: key, Hash: hash}); err != nil {
				return err
			}
		}
		return nil
	}))
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

// DeleteUnlinkedArtwork deletes the artwork which no object links to,
// which was last put before the time, and returns how many were deleted
func (self *Store) DeleteUnlinkedArtwork(ctx context.Context, before time.Time) (_ int, err error) {
	ctx, end := self.span(ctx, "DeleteUnlinkedArtwork", attribute.String("arg.before", before.UTC().Format(time.RFC3339)))
	defer func() { end(err) }()
	var deleted schema.ArtworkList
	if err := self.conn.Delete(ctx, &deleted, schema.ArtworkUnlinked{Before: before.UTC()}); errors.Is(err, sqlite.ErrNotFound) {
		return 0, nil
	} else if err != nil {
		return 0, storeErr(err)
	}
	return len(deleted), nil
}
