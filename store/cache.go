package store

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// CacheStore keeps what a volume's backend gets from a service, by key, on
// disk, so it isn't kept in memory, nor got again after a restart. An entry
// is kept for its TTL, or until it's replaced, or deleted, or the volume
// changes, when the registry deletes the store. A backend gets its volume's
// store as an argument, which it uses, or not, and must work without any of
// its entries, so an entry is never the only copy of anything. It's safe to
// use from more than one goroutine.
type CacheStore interface {
	// Get returns the entry of the key, or ErrNotFound when there's none, or
	// it expired. The caller closes it.
	Get(ctx context.Context, key string) (CacheEntry, error)

	// Put stores the content, as it's read, as the entry of the key, with its
	// metadata, for the TTL, or until it's replaced, or deleted, when it's
	// zero, replacing any, once it's all read, and returns it, to read back,
	// which the caller closes. When the content can't be read, any entry of
	// the key is kept.
	Put(ctx context.Context, key string, ttl time.Duration, meta CacheMeta, r io.Reader) (CacheEntry, error)

	// GetOrCreate returns the entry of the key when there's one of the
	// version, or else calls create, which writes the content to w, and
	// stores it, with the metadata create returns, whose
	// version is the version, when it hasn't one, for the TTL, and returns
	// it, which the caller closes. A key is created once, however many ask for
	// it at once, which wait for it.
	GetOrCreate(ctx context.Context, key, version string, ttl time.Duration, create func(w io.Writer) (CacheMeta, error)) (CacheEntry, error)

	// Delete deletes the entry of the key, or every entry beneath it, when it
	// ends with "/", such as "export/", which isn't an error when there's
	// none. An entry which is being read is deleted once it's closed.
	Delete(ctx context.Context, key string) error
}

// CacheMeta is what an entry is of
type CacheMeta struct {
	Version string            `json:"version,omitempty"` // Such as a revision, an entity tag, or a page token
	Values  map[string]string `json:"values,omitempty"`  // Small values, such as a feed's Last-Modified
}

// CacheEntry is an entry's content, which can seek, and be read at an
// offset, as a file can, and what it is
type CacheEntry interface {
	io.ReadSeekCloser
	io.ReaderAt

	// Meta returns what the entry is of
	Meta() CacheMeta

	// Size returns how many bytes the content is
	Size() int64

	// Stored returns when it was stored
	Stored() time.Time

	// Expires returns when it isn't returned, or zero, without a TTL
	Expires() time.Time
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// GetCacheJSON decodes the entry of the key, which is JSON, into v, and
// returns what it's of, or ErrNotFound, when there's none, or it expired
func GetCacheJSON(ctx context.Context, cache CacheStore, key string, v any) (CacheMeta, error) {
	entry, err := cache.Get(ctx, key)
	if err != nil {
		return CacheMeta{}, err
	}
	defer entry.Close()
	if err := json.NewDecoder(entry).Decode(v); err != nil {
		return CacheMeta{}, gofiler.ErrInternalServerError.Withf("cache %q: %v", key, err)
	}
	return entry.Meta(), nil
}

// PutCacheJSON stores v, as JSON, as the entry of the key, with its
// metadata, for the TTL, or until it's replaced, or deleted, when it's zero
func PutCacheJSON(ctx context.Context, cache CacheStore, key string, ttl time.Duration, meta CacheMeta, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return gofiler.ErrBadParameter.Withf("cache %q: %v", key, err)
	}
	entry, err := cache.Put(ctx, key, ttl, meta, bytes.NewReader(data))
	if err != nil {
		return err
	}
	return entry.Close()
}
