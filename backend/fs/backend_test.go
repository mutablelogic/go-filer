package fs_test

import (
	"context"
	"net/url"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fs "github.com/mutablelogic/go-filer/backend/fs"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_Backend(t *testing.T) {
	// The backend of directories describes itself, with its icon, and the
	// options of a volume which aren't set
	b := fs.Backend()
	assert.Equal(t, "file", b.Scheme())
	assert.Equal(t, "Server filesystem", b.Description())
	if assert.NotNil(t, b.Icon()) {
		assert.Equal(t, "image/svg+xml", b.Icon().Type())
		assert.Contains(t, string(b.Icon().Data()), "<svg")
	}
	defaults := b.Defaults()
	assert.Equal(t, "false", defaults.Get("readonly"))
	assert.NotEmpty(t, defaults["skip"])
	assert.NotEmpty(t, defaults["package"])
	assert.False(t, defaults.Has("description"))

	// It opens a directory, of an absolute path, and its name
	dir := t.TempDir()
	f, err := b.New(context.Background(), &url.URL{Scheme: "file", Host: "media", Path: dir}, nil, gofiler.Opts{})
	require.NoError(t, err)
	if closer, ok := f.(interface{ Close() error }); ok {
		assert.NoError(t, closer.Close())
	}
	_, err = b.New(context.Background(), &url.URL{Scheme: "file", Host: "media", Path: "relative"}, nil, gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = b.New(context.Background(), &url.URL{Scheme: "file", Host: "1media", Path: dir}, nil, gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}
