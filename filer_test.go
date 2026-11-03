package filer_test

import (
	"net/url"
	"path/filepath"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_PatchValues(t *testing.T) {
	values := url.Values{"a": {"1"}, "b": {"2", "3"}, "c": {"4"}}

	// Each key of the patch replaces the option's values, or removes it, and
	// neither is changed
	result := gofiler.PatchValues(values, url.Values{"b": {"5"}, "c": nil, "d": {"6", "7"}})
	assert.Equal(t, url.Values{"a": {"1"}, "b": {"5"}, "d": {"6", "7"}}, result)
	assert.Equal(t, url.Values{"a": {"1"}, "b": {"2", "3"}, "c": {"4"}}, values)
	result["a"][0] = "changed"
	assert.Equal(t, "1", values["a"][0])
	assert.Equal(t, values, gofiler.PatchValues(values, nil))
}

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
		u, err := gofiler.ParseURL(value)
		require.NoError(t, err, value)
		assert.Equal(t, want, u.String(), value)
	}

	// But not nothing
	for _, value := range []string{"", "?readonly=true"} {
		_, err := gofiler.ParseURL(value)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, value)
	}
}
