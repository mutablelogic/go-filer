package filer

import (
	"context"
	"io"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// CacheStore keeps what a volume's backend gets from a service, by key, on
// disk. An entry is kept for its TTL, or until it's replaced, or deleted,
// or the volume changes, when the registry deletes the store. A backend gets its volume's
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

	// GetOrCreate is how a backend caches something it fetches, such as a
	// Google Doc it exports: it returns the cached entry of the key when
	// that's of the version the backend wants, so it isn't fetched again,
	// and otherwise fetches it, by calling valuefn, which writes the content
	// to w, as it's fetched, and stores it, for the TTL. The entry's
	// metadata is what valuefn returns, with the version, unless valuefn
	// sets another. When several callers ask for a key at once, valuefn is
	// called once, and the others wait for it, so a key is fetched once.
	// The caller closes the entry which is returned.
	GetOrCreate(ctx context.Context, key, version string, ttl time.Duration, valuefn func(w io.Writer) (CacheMeta, error)) (CacheEntry, error)

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
