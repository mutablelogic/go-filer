package schema

import (
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer/schema"
	sqlite "github.com/mutablelogic/go-sqlite"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ArtworkPut is artwork which is stored, of its hash, or the artwork of its
// hash, which is stored already, as it's linked to
type ArtworkPut filer.ArtworkMeta

// ArtworkHash selects artwork, of its hash, which is got, with its data,
// when it's asked for
type ArtworkHash struct {
	Hash string
	Data bool
}

// ArtworkUnlinked selects the artwork which no object links to, which is
// deleted
type ArtworkUnlinked struct{}

// ArtworkList is the artwork which is read, of a get, which reads one, a
// list, or a delete
type ArtworkList []*filer.Artwork

// ObjectArtwork selects an object, of its volume, and key, of which its
// links to artwork are listed, or deleted, and the object is got, so it's
// found, or not
type ObjectArtwork struct {
	Volume, Key string
}

// ObjectArtworkLink is a link of an object to artwork, of its hash, which is
// inserted
type ObjectArtworkLink struct {
	Volume, Key, Hash string
}

// VolumeArtwork selects the links of the objects of a volume to their
// artwork, which are deleted, as what a task found of them is
type VolumeArtwork string

// Keys are the keys of objects, or the hashes of artwork, which are read,
// of which rows a query returned
type Keys []string

////////////////////////////////////////////////////////////////////////////////
// SELECTORS

func (h ArtworkHash) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	bind.Set("hash", h.Hash)
	bind.Set("data", h.Data)
	switch op {
	case sqlite.Get:
		return bind.Query("artwork.get"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("artwork: %v", op)
}

func (ArtworkUnlinked) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	switch op {
	case sqlite.Delete:
		return bind.Query("artwork.delete_unlinked"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("unlinked artwork: %v", op)
}

func (o ObjectArtwork) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	bind.Set("volume", o.Volume)
	bind.Set("key", o.Key)
	switch op {
	case sqlite.Get:
		return bind.Query("object_artwork.object"), nil
	case sqlite.List:
		return bind.Query("object_artwork.list"), nil
	case sqlite.Delete:
		return bind.Query("object_artwork.delete"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("object artwork: %v", op)
}

func (v VolumeArtwork) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	bind.Set("volume", string(v))
	switch op {
	case sqlite.Delete:
		return bind.Query("object_artwork.delete_volume"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("volume artwork: %v", op)
}

////////////////////////////////////////////////////////////////////////////////
// WRITERS

// Insert binds artwork, of its hash, and returns the query which stores it
func (put ArtworkPut) Insert(bind *sqlite.Bind) (string, error) {
	bind.Set("hash", filer.ArtworkMeta(put).Hash())
	bind.Set("type", put.Type)
	bind.Set("width", put.Width)
	bind.Set("height", put.Height)
	bind.Set("context", put.Context)
	bind.Set("data", put.Data)
	bind.Set("now", time.Now().UTC())
	return bind.Query("artwork.put"), nil
}

func (ArtworkPut) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("artwork isn't updated, as it's of its hash")
}

// Insert binds a link, and returns the query which inserts it
func (link ObjectArtworkLink) Insert(bind *sqlite.Bind) (string, error) {
	bind.Set("volume", link.Volume)
	bind.Set("key", link.Key)
	bind.Set("hash", link.Hash)
	return bind.Query("object_artwork.insert"), nil
}

func (ObjectArtworkLink) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("a link to artwork isn't updated")
}

////////////////////////////////////////////////////////////////////////////////
// READERS

func (list *ArtworkList) Scan(row sqlite.Row) error {
	var artwork filer.Artwork
	if err := row.Scan(&artwork.Hash, &artwork.Type, &artwork.Width, &artwork.Height, &artwork.Context, &artwork.Size, &artwork.CreatedAt, &artwork.Data); err != nil {
		return err
	}
	*list = append(*list, &artwork)
	return nil
}

func (keys *Keys) Scan(row sqlite.Row) error {
	var key string
	if err := row.Scan(&key); err != nil {
		return err
	}
	*keys = append(*keys, key)
	return nil
}
