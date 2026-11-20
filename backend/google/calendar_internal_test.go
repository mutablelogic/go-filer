package google

import (
	"context"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// Test_LockWrites tests a write, or a sync, waits for the one which holds
// the lock, until its context is cancelled, when it returns at once, and
// then the next one starts
func Test_LockWrites(t *testing.T) {
	filer := &CalendarFiler{writing: make(chan struct{}, 1)}
	unlock, err := filer.lockWrites(t.Context())
	require.NoError(t, err)

	// One which waits stops when its context is cancelled
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = filer.lockWrites(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second)

	// Once it's unlocked, the next one starts
	unlock()
	unlock, err = filer.lockWrites(t.Context())
	require.NoError(t, err)
	unlock()
}

// Test_ListingGeneration tests a listing which was got before the listings
// were deleted isn't saved, as it may be out of date
func Test_ListingGeneration(t *testing.T) {
	cache, err := sqlite.NewCacheStore(t.Context(), gosqlite.Memory)
	require.NoError(t, err)
	defer cache.Close()
	filer := &CalendarFiler{cache: cache}
	from, to := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)

	// One which was got before they were deleted isn't saved
	generation := filer.listingGeneration()
	require.NoError(t, filer.dropListings(t.Context(), "/2026/2026-11/20-x-e1.ics"))
	filer.saveListing(t.Context(), "/2026/2026-11", from, to, nil, generation)
	_, err = cache.Get(t.Context(), listingKey("/2026/2026-11"))
	assert.ErrorIs(t, err, gofiler.ErrNotFound)

	// One which was got after is
	filer.saveListing(t.Context(), "/2026/2026-11", from, to, nil, filer.listingGeneration())
	entry, err := cache.Get(t.Context(), listingKey("/2026/2026-11"))
	require.NoError(t, err)
	entry.Close()
}
