package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	// Packages
	kong "github.com/alecthomas/kong"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_ObjectListCmd(t *testing.T) {
	var cli struct {
		ObjectList ObjectListCmd `cmd:"" name:"objects"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	// The volume, and the path, which is optional, are the arguments, and
	// the rest of the request is flags
	_, err = parser.Parse([]string{"objects", "media", "/music", "-r", "--start-after", "/music/a.mp3", "--limit", "10"})
	require.NoError(t, err)
	req := cli.ObjectList.request()
	assert.Equal(t, "media", req.Name)
	assert.Equal(t, "/music", req.Path)
	assert.True(t, req.Recursive)
	assert.Equal(t, "/music/a.mp3", req.StartAfter)
	if assert.NotNil(t, req.Limit) {
		assert.Equal(t, uint64(10), *req.Limit)
	}

	_, err = parser.Parse([]string{"objects", "media"})
	require.NoError(t, err)
	req = cli.ObjectList.request()
	assert.Equal(t, "", req.Path)
	assert.Nil(t, req.Limit)

	// The volume is needed
	_, err = parser.Parse([]string{"objects"})
	assert.Error(t, err)
}

func Test_ObjectDeleteCmd(t *testing.T) {
	var cli struct {
		ObjectDelete ObjectDeleteCmd `cmd:"" name:"object-delete"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	// The volume and path are needed, and recursive is a flag
	_, err = parser.Parse([]string{"object-delete", "media", "/music", "-r"})
	require.NoError(t, err)
	assert.Equal(t, ObjectDeleteCmd{Name: "media", Path: "/music", Recursive: true}, cli.ObjectDelete)
	_, err = parser.Parse([]string{"object-delete", "media"})
	assert.Error(t, err)
}

func Test_ObjectPutCmd(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "song.mp3")
	require.NoError(t, os.WriteFile(file, []byte("abc"), 0o644))
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, os.Chtimes(file, modtime, modtime))
	request := func(args ...string) (string, time.Time, error) {
		var cli struct {
			ObjectPut ObjectPutCmd `cmd:"" name:"object-put"`
		}
		parser, err := kong.New(&cli)
		require.NoError(t, err)
		if _, err := parser.Parse(append([]string{"object-put"}, args...)); err != nil {
			return "", time.Time{}, err
		}
		req, closer, err := cli.ObjectPut.request()
		if err != nil {
			return "", time.Time{}, err
		}
		closer.Close()
		return req.Path, req.ModTime, nil
	}

	// The file is uploaded to the path, or to a directory with its name,
	// with its modified time
	path, mtime, err := request("media", "/music/a.mp3", file)
	require.NoError(t, err)
	assert.Equal(t, "/music/a.mp3", path)
	assert.True(t, modtime.Equal(mtime))
	path, _, err = request("media", "/music/", file)
	require.NoError(t, err)
	assert.Equal(t, "/music/song.mp3", path)
	path, _, err = request("media", "/music/a.mp3", "-")
	require.NoError(t, err)
	assert.Equal(t, "/music/a.mp3", path)

	// Errors
	for _, args := range [][]string{
		{"media", "/music/", "-"},
		{"media", "/a.mp3", filepath.Join(dir, "missing")},
		{"media", "/a.mp3", dir},
		{"media", "/a.mp3", file, "--no-replace", "--replace-only"},
		{"media", "/a.mp3"},
	} {
		_, _, err := request(args...)
		assert.Error(t, err, args)
	}
}

func Test_ObjectUploadCmd(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.mp3", "album/b.mp3", "album/disc/c.mp3"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, filepath.Dir(name)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0o644))
	}
	files := func(args ...string) ([]string, error) {
		var cli struct {
			ObjectUpload ObjectUploadCmd `cmd:"" name:"object-upload"`
		}
		parser, err := kong.New(&cli)
		require.NoError(t, err)
		if _, err := parser.Parse(append([]string{"object-upload", "media", "/music"}, args...)); err != nil {
			return nil, err
		}
		files, err := cli.ObjectUpload.files()
		var paths []string
		for _, file := range files {
			paths = append(paths, file.path)
			info, err := os.Stat(file.name)
			require.NoError(t, err)
			assert.Equal(t, info.Size(), file.size, file.path)
		}
		return paths, err
	}

	// A file is uploaded with its name, and a directory with its files, in
	// a directory of its name
	paths, err := files(filepath.Join(dir, "a.mp3"), filepath.Join(dir, "album"))
	require.NoError(t, err)
	assert.Equal(t, []string{"a.mp3", "album/b.mp3", "album/disc/c.mp3"}, paths)

	// A directory which ends with a separator is its files, without it
	paths, err = files(filepath.Join(dir, "album") + "/")
	require.NoError(t, err)
	assert.Equal(t, []string{"b.mp3", "disc/c.mp3"}, paths)
	paths, err = files(dir + string(filepath.Separator))
	require.NoError(t, err)
	assert.Equal(t, []string{"a.mp3", "album/b.mp3", "album/disc/c.mp3"}, paths)

	// -y is --overwrite
	var cli struct {
		Upload ObjectUploadCmd `cmd:"" name:"object-upload"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)
	_, err = parser.Parse([]string{"object-upload", "media", "/music", filepath.Join(dir, "a.mp3"), "-y"})
	require.NoError(t, err)
	assert.True(t, cli.Upload.Overwrite)

	// A file which doesn't exist, or none, isn't uploaded
	_, err = files(filepath.Join(dir, "missing"))
	assert.Error(t, err)
	_, err = files()
	assert.Error(t, err)
}

func Test_createTemp(t *testing.T) {
	dir := t.TempDir()

	// The temporary file has the permissions of a file which is created,
	// which the umask applies to
	created, err := os.Create(filepath.Join(dir, "created.txt"))
	require.NoError(t, err)
	created.Close()
	tmp, err := createTemp(filepath.Join(dir, "song.mp3"))
	require.NoError(t, err)
	defer tmp.Close()
	want, err := os.Stat(created.Name())
	require.NoError(t, err)
	got, err := tmp.Stat()
	require.NoError(t, err)
	assert.Equal(t, want.Mode().Perm(), got.Mode().Perm())
	assert.True(t, strings.HasPrefix(filepath.Base(tmp.Name()), ".song.mp3."), tmp.Name())
}

func Test_ParseMeta(t *testing.T) {
	// The metadata is [NAMESPACE.]KEY=VALUE, whose value is JSON, when it is
	meta, err := parseMeta([]string{"photo.camera=X100V", "rating=5", "flag=true", `title="quoted"`, "empty=", "a.b.c=1=2"})
	require.NoError(t, err)
	require.Len(t, meta, 6)
	for i, want := range []struct{ namespace, key, value string }{
		{"photo", "camera", `"X100V"`}, {"", "rating", `5`}, {"", "flag", `true`}, {"", "title", `"quoted"`}, {"", "empty", `""`}, {"a", "b.c", `"1=2"`},
	} {
		assert.Equal(t, want.namespace, meta[i].Namespace)
		assert.Equal(t, want.key, meta[i].Key)
		assert.JSONEq(t, want.value, string(meta[i].Value))
	}
	meta, err = parseMeta(nil)
	require.NoError(t, err)
	assert.Nil(t, meta)
	for _, flag := range []string{"rating", "=5", "photo.=5", ".rating=5", ".=5"} {
		_, err := parseMeta([]string{flag})
		assert.Error(t, err, flag)
	}
}
