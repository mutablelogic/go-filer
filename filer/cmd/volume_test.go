package cmd

import (
	"os"
	"path/filepath"
	"testing"

	// Packages
	kong "github.com/alecthomas/kong"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_VolumeCreateCmd(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
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

	// volume parses the arguments, and returns the volume to create
	volume := func(args ...string) (string, string, error) {
		var cli struct {
			VolumeCreate VolumeCreateCmd `cmd:"" name:"volume-create"`
		}
		parser, err := kong.New(&cli)
		require.NoError(t, err)
		if _, err := parser.Parse(append([]string{"volume-create"}, args...)); err != nil {
			return "", "", err
		}
		volume, err := cli.VolumeCreate.volume()
		return volume.Name, volume.URL, err
	}

	// The name is the URL's host, or --name, which is the host of a URL
	// without one, and a directory is a file URL, which is absolute
	for _, test := range []struct {
		args       []string
		name, want string
	}{
		{[]string{"file://media/mnt/media"}, "media", "file://media/mnt/media"},
		{[]string{"file://media/mnt/media", "--name", "media"}, "media", "file://media/mnt/media"},
		{[]string{"file:///mnt/media", "--name", "media"}, "media", "file://media/mnt/media"},
		{[]string{"/mnt/media", "--name", "media"}, "media", "file://media" + abs(filepath.FromSlash("/mnt/media"))},
		{[]string{"/mnt/media?readonly=true", "--name", "media"}, "media", "file://media" + abs(filepath.FromSlash("/mnt/media")) + "?readonly=true"},
		{[]string{"music", "--name", "media"}, "media", "file://media" + abs(filepath.Join(wd, "music"))},
		{[]string{"ftp://media/data"}, "media", "ftp://media/data"},
	} {
		name, url, err := volume(test.args...)
		require.NoError(t, err, test.args)
		assert.Equal(t, test.name, name, test.args)
		assert.Equal(t, test.want, url, test.args)
	}

	// Options are added to those of the URL, and can be repeated, and an
	// option without a value is empty
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"file://media/mnt/media", "--opt", "readonly=true"}, "readonly=true"},
		{[]string{"file://media/mnt/media?readonly=true", "-o", "package="}, "package=&readonly=true"},
		{[]string{"file://media/mnt/media", "--opt", "readonly"}, "readonly="},
		{[]string{"file://media/mnt/media", "--opt", `skip=^\.`, "--opt", `skip=\.tmp$`}, "skip=%5E%5C.&skip=%5C.tmp%24"},
	} {
		_, url, err := volume(test.args...)
		require.NoError(t, err, test.args)
		assert.Equal(t, "file://media/mnt/media?"+test.want, url, test.args)
	}

	// A volume without a name, or whose name isn't the URL's host, or with
	// an option without a key, isn't created, and the URL is needed
	for _, args := range [][]string{
		{"file:///mnt/media"},
		{"/mnt/media"},
		{"file://media/mnt/media", "--name", "other"},
		{"file://media/mnt/media", "--opt", "=true"},
		{},
	} {
		_, _, err := volume(args...)
		assert.Error(t, err, args)
	}
}

func Test_VolumeUpdateCmd(t *testing.T) {
	patch := func(args ...string) (map[string]string, error) {
		var cli struct {
			VolumeUpdate VolumeUpdateCmd `cmd:"" name:"volume-update"`
		}
		parser, err := kong.New(&cli)
		require.NoError(t, err)
		if _, err := parser.Parse(append([]string{"volume-update", "media"}, args...)); err != nil {
			return nil, err
		}
		patch, err := cli.VolumeUpdate.patch()
		if err != nil {
			return nil, err
		}
		result := map[string]string{}
		for key, value := range patch {
			result[key] = string(value)
		}
		return result, nil
	}

	// Each option has the values it's given, where one without a value has
	// none, and an option to remove is null
	got, err := patch("-o", "readonly=true", "-o", `skip=^\.`, "-o", `skip=\.tmp$`, "-o", "package", "--unset", "other")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"readonly": `["true"]`,
		"skip":     `["^\\.","\\.tmp$"]`,
		"package":  `[]`,
		"other":    `null`,
	}, got)

	// No options, an option without a key, or one which is set and removed,
	// isn't a patch
	for _, args := range [][]string{{}, {"-o", "=true"}, {"--unset", ""}, {"-o", "readonly=true", "--unset", "readonly"}} {
		_, err := patch(args...)
		assert.Error(t, err, args)
	}
}
