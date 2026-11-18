package sqlite_test

import (
	"context"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	sqlite "github.com/mutablelogic/go-filer/filer/indexer/sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_Store(t *testing.T) {
	// It needs its path, and isn't implemented yet, so each method says so,
	// until it runs storetest
	_, err := sqlite.New("")
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	store, err := sqlite.New("x")
	require.NoError(t, err)
	ctx := context.Background()
	_, err = store.Get(ctx, "media", "/")
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	assert.ErrorIs(t, store.Put(ctx, "media"), gofiler.ErrNotImplemented)
	assert.ErrorIs(t, store.Delete(ctx, "media", "/"), gofiler.ErrNotImplemented)
	_, err = store.List(ctx, "media", indexer.ListRequest{Parent: "/"})
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	_, err = store.State(ctx, "media")
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	assert.ErrorIs(t, store.SetState(ctx, "media", indexer.State{}), gofiler.ErrNotImplemented)
}
