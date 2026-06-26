package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ArtworkKey is the key of an artwork, which is a hash of the artwork data
type ArtworkKey string

// ArtworkInfoKey selects artwork metadata only (no binary data)
type ArtworkInfoKey ArtworkKey

// ArtworkMeta represents metadata for one or more thumbnails, which are attached to objects
// and keyed by their etag
type ArtworkMeta struct {
	Data   []byte `json:"data"`
	Type   string `json:"type"`
	Width  uint64 `json:"width"`
	Height uint64 `json:"height"`
}

// Artwork represents one or more thumbnails, which are attached to objects
// and keyed by their etag
type Artwork struct {
	ETag ArtworkKey `json:"key"`
	ArtworkMeta
	CreatedAt time.Time `json:"created_at"`
}

// ArtworkInfo is artwork metadata without the binary data, used for conditional requests.
type ArtworkInfo struct {
	ETag      ArtworkKey `json:"key"`
	Type      string     `json:"type"`
	Width     uint64     `json:"width"`
	Height    uint64     `json:"height"`
	CreatedAt time.Time  `json:"created_at"`
}

// ArtworkUploadRequest represents a request to upload an artwork
type ArtworkUploadRequest struct {
	Data types.File `json:"data" validate:"required"`
}

// GetArtworkRequest represents a request to get artwork
type GetArtworkRequest struct {
	Key             ArtworkKey `json:"key" validate:"required"`
	IfModifiedSince *time.Time `json:"if_modified_since,omitempty"`
	IfNoneMatch     *string    `json:"if_none_match,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (o Artwork) String() string {
	return types.Stringify(o)
}

func (k ArtworkMeta) String() string {
	return types.Stringify(k)
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (k ArtworkInfoKey) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if k == "" {
		return "", httpresponse.ErrBadRequest.With("missing artwork key")
	}
	bind.Set("etag", k)
	switch op {
	case pg.Get:
		return bind.Query("filer.artwork_get_meta"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported ArtworkInfoKey operation %q", op)
	}
}

func (k ArtworkKey) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if k == "" {
		return "", httpresponse.ErrBadRequest.With("missing artwork key")
	} else {
		bind.Set("etag", k)
	}
	switch op {
	case pg.Get:
		return bind.Query("filer.artwork_get"), nil
	case pg.Delete:
		return bind.Query("filer.artwork_delete"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported ArtworkKey operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (o ArtworkMeta) Insert(bind *pg.Bind) (string, error) {
	if len(o.Data) == 0 {
		return "", gofiler.ErrBadParameter.With("missing artwork data")
	} else {
		bind.Set("data", o.Data)
	}
	if o.Type == "" {
		return "", gofiler.ErrBadParameter.With("missing content type")
	} else {
		bind.Set("type", o.Type)
	}
	if o.Width == 0 {
		return "", gofiler.ErrBadParameter.With("missing artwork width")
	} else {
		bind.Set("width", o.Width)
	}
	if o.Height == 0 {
		return "", gofiler.ErrBadParameter.With("missing artwork height")
	} else {
		bind.Set("height", o.Height)
	}

	// Calculate the etag from the data
	h := sha256.Sum256(o.Data)
	bind.Set("etag", hex.EncodeToString(h[:]))

	// Return the query
	return bind.Query("filer.artwork_upsert"), nil
}

func (o ArtworkMeta) Update(bind *pg.Bind) error {
	return gofiler.ErrNotImplemented.With("ArtworkMeta update is not supported")
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (o *Artwork) Scan(row pg.Row) error {
	return row.Scan(
		&o.ETag,
		&o.Data,
		&o.Type,
		&o.Width,
		&o.Height,
		&o.CreatedAt,
	)
}

func (o *ArtworkInfo) Scan(row pg.Row) error {
	return row.Scan(
		&o.ETag,
		&o.Type,
		&o.Width,
		&o.Height,
		&o.CreatedAt,
	)
}
