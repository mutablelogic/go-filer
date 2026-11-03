//go:build !client

package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	// Packages
	kong "github.com/alecthomas/kong"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_ServerWorker(t *testing.T) {
	parse := func(args ...string) Flags {
		t.Helper()
		var cli struct {
			Flags
		}
		parser, err := kong.New(&cli)
		require.NoError(t, err)
		_, err = parser.Parse(args)
		require.NoError(t, err)
		return cli.Flags
	}

	// The worker is the flag's, or the environment's, FILER_WORKER before
	// WORKER, and is otherwise empty, when it's the hostname
	t.Setenv("FILER_WORKER", "")
	t.Setenv("WORKER", "")
	require.NoError(t, os.Unsetenv("FILER_WORKER"))
	require.NoError(t, os.Unsetenv("WORKER"))
	assert.Empty(t, parse().Worker)
	assert.Equal(t, "worker-1", parse("--worker", "worker-1").Worker)
	t.Setenv("WORKER", "worker-3")
	assert.Equal(t, "worker-3", parse().Worker)
	t.Setenv("FILER_WORKER", "worker-2")
	assert.Equal(t, "worker-2", parse().Worker)
	assert.Equal(t, "worker-1", parse("--worker", "worker-1").Worker)
}

func Test_VolumeURL(t *testing.T) {
	// abs returns the URL path of a directory, which is absolute
	abs := func(path string) string {
		path, err := filepath.Abs(filepath.FromSlash(path))
		require.NoError(t, err)
		if path = filepath.ToSlash(path); path[0] != '/' {
			path = "/" + path
		}
		return path
	}

	// A directory is named by its base name, or its name option, which is
	// its host, and a URL keeps its own
	for volume, want := range map[string]string{
		"/Volumes/Scratch/tmp":              "file://tmp" + abs("/Volumes/Scratch/tmp"),
		"/mnt/media?readonly=true":          "file://media" + abs("/mnt/media") + "?readonly=true",
		"/Volumes/My Disk?name=disk":        "file://disk" + strings.ReplaceAll(abs("/Volumes/My Disk"), " ", "%20"),
		"file://media/mnt/media":            "file://media/mnt/media",
		"s3://my.bucket/photos?name=photos": "s3://my.bucket/photos?name=photos",
	} {
		u, err := volumeURL(volume)
		require.NoError(t, err, volume)
		assert.Equal(t, want, u.String(), volume)
	}

	// But not a directory whose name isn't valid, without its name option,
	// or whose options are malformed, or a URL which can't be parsed, and
	// the error doesn't have its credentials
	for _, volume := range []string{"/Volumes/My Disk", "/mnt/2026", "", "/mnt/media?name=media&readonly=%ZZ", "s3://user:secret@bucket:bad"} {
		_, err := volumeURL(volume)
		if assert.Error(t, err, volume) {
			assert.NotContains(t, err.Error(), "secret", volume)
		}
	}
}
