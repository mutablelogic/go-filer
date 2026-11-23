package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	// Packages
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ArtworkMeta is artwork, which is an image: its type, its size, in pixels,
// its context, which is what it is of an object, such as a cover, and its
// data
type ArtworkMeta struct {
	Type    string `json:"type" help:"Its content type, such as image/jpeg" example:"image/jpeg"`
	Width   uint   `json:"width" help:"Its width, in pixels" example:"512"`
	Height  uint   `json:"height" help:"Its height, in pixels" example:"512"`
	Context string `json:"context" help:"What it is of an object, such as cover, back or thumbnail" example:"cover"`
	Data    []byte `json:"data,omitempty" help:"Its data, which is only returned when it's asked for"`
}

// Artwork is artwork which is stored, by its hash, which is of its context
// and data, with the size of its data, and when it was stored
type Artwork struct {
	ArtworkMeta
	Hash      string    `json:"hash" readonly:"" help:"The SHA-256 hash of its context and data, in hex"`
	Size      uint64    `json:"size" readonly:"" help:"The size of its data, in bytes"`
	CreatedAt time.Time `json:"created_at,omitzero" readonly:"" help:"When it was first stored"`
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// ArtworkCover, ArtworkBack and ArtworkThumbnail are contexts of
	// artwork: the front cover of an album, or a book, its back, and an
	// image of an object, which is smaller than it
	ArtworkCover     = "cover"
	ArtworkBack      = "back"
	ArtworkThumbnail = "thumbnail"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Hash returns the hash of the artwork, which identifies it: the SHA-256
// hash, in hex, of its context, and its data, so the same image of two
// contexts is two artworks
func (a ArtworkMeta) Hash() string {
	hash := sha256.New()
	hash.Write([]byte(a.Context))
	hash.Write([]byte{0})
	hash.Write(a.Data)
	return hex.EncodeToString(hash.Sum(nil))
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (a ArtworkMeta) String() string {
	a.Data = nil
	return types.Stringify(a)
}

func (a Artwork) String() string {
	a.Data = nil
	return types.Stringify(a)
}
