package registry_test

import (
	"path/filepath"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	registry "github.com/mutablelogic/go-filer/registry"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_ParseURL(t *testing.T) {
	// abs returns the URL path of a directory, which is absolute, which on
	// Windows has a drive, so /mnt/media is /C:/mnt/media
	abs := func(path string) string {
		path, err := filepath.Abs(path)
		require.NoError(t, err)
		if path = filepath.ToSlash(path); path[0] != '/' {
			path = "/" + path
		}
		return path
	}

	// A URL is itself, and a directory is a file URL, without a host, with
	// its options, made absolute
	for value, want := range map[string]string{
		"s3://my.bucket/photos?name=photos": "s3://my.bucket/photos?name=photos",
		"file://media/mnt/media":            "file://media/mnt/media",
		"/mnt/media":                        "file://" + abs(filepath.FromSlash("/mnt/media")),
		"/mnt/media?readonly=true":          "file://" + abs(filepath.FromSlash("/mnt/media")) + "?readonly=true",
		"media":                             "file://" + abs("media"),
		"./photos/../media":                 "file://" + abs("media"),
		"C:/media":                          "file://" + abs("C:/media"),
	} {
		u, err := registry.ParseURL(value)
		require.NoError(t, err, value)
		assert.Equal(t, want, u.String(), value)
	}

	// But not nothing, or a URL which can't be parsed, which isn't a
	// directory, without its credentials in the error
	for _, value := range []string{"", "?readonly=true", "s3://user:secret@bucket:bad"} {
		_, err := registry.ParseURL(value)
		if assert.ErrorIs(t, err, gofiler.ErrBadParameter, value) {
			assert.NotContains(t, err.Error(), "secret", value)
		}
	}
}
