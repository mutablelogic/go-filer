package pg_test

import (
	"context"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	pg "github.com/mutablelogic/go-filer/store/pg"
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
	s, err := pg.New(conn{})
	require.NoError(t, err)
	ctx := context.Background()
	_, err = s.GetObject(ctx, "media", "/")
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	assert.ErrorIs(t, s.PutObject(ctx, "media"), gofiler.ErrNotImplemented)
	assert.ErrorIs(t, s.DeleteObject(ctx, "media", "/"), gofiler.ErrNotImplemented)
	_, err = s.ListObjects(ctx, "media", store.ListRequest{Parent: "/"})
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	_, err = s.State(ctx, "media")
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	assert.ErrorIs(t, s.SetState(ctx, "media", schema.IndexState{}), gofiler.ErrNotImplemented)
	_, err = s.CreateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/tmp"})
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	_, err = s.UpdateVolume(ctx, schema.VolumeMeta{Name: "media", URL: "file://media/tmp"})
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	_, err = s.GetVolume(ctx, "media")
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	_, err = s.ListVolumes(ctx)
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	assert.ErrorIs(t, s.DeleteVolume(ctx, "media"), gofiler.ErrNotImplemented)
}
