package store

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// GetCacheJSON decodes the entry of the key, which is JSON, into v, and
// returns what it's of, or ErrNotFound, when there's none, or it expired
func GetCacheJSON(ctx context.Context, cache gofiler.CacheStore, key string, v any) (gofiler.CacheMeta, error) {
	entry, err := cache.Get(ctx, key)
	if err != nil {
		return gofiler.CacheMeta{}, err
	}
	defer entry.Close()
	if err := json.NewDecoder(entry).Decode(v); err != nil {
		return gofiler.CacheMeta{}, gofiler.ErrInternalServerError.Withf("cache %q: %v", key, err)
	}
	return entry.Meta(), nil
}

// PutCacheJSON stores v, as JSON, as the entry of the key, with its
// metadata, for the TTL, or until it's replaced, or deleted, when it's zero
func PutCacheJSON(ctx context.Context, cache gofiler.CacheStore, key string, ttl time.Duration, meta gofiler.CacheMeta, v any) error {
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
