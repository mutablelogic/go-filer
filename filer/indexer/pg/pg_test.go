package pg_test

import (
	"context"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	pg "github.com/mutablelogic/go-filer/filer/indexer/pg"
	gopg "github.com/mutablelogic/go-pg"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// conn is a pool of connections, which isn't used, as the store isn't
// implemented yet
type conn struct {
	gopg.PoolConn
}

func Test_Store(t *testing.T) {
	// It needs a connection, and isn't implemented yet, so each method says
	// so, until it runs storetest, of a test database
	_, err := pg.New(nil)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	store, err := pg.New(conn{})
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
