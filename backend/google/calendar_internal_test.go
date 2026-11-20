package google

import (
	"context"
	"testing"
	"time"

	// Packages
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
