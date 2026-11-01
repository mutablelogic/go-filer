package httpclient_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/filer"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	httphandler "github.com/mutablelogic/go-filer/filer/httphandler"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

///////////////////////////////////////////////////////////////////////////////
// HELPERS

// newClient returns a client for a test server serving the filer API, for a
// registry with no volumes, and makes the directory of its volumes, which
// is made first, so it's removed after the registry is closed, as on
// Windows, a directory which is open can't be removed
func newClient(t *testing.T) (*httpclient.Client, func() string) {
	t.Helper()
	base := t.TempDir()
	registry, err := filer.New(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { registry.Close() })

	mux := http.NewServeMux()
	router, err := httprouter.NewRouter(context.Background(), mux, "/api", "", "test", "0.0.0")
	require.NoError(t, err)
	require.NoError(t, httphandler.RegisterHandlers(registry, router))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client, err := httpclient.New(server.URL + "/api")
	require.NoError(t, err)
	return client, func() string {
		dir, err := os.MkdirTemp(base, "volume-")
		require.NoError(t, err)
		return dir
	}
}

// fileURL returns the URL of a volume for the directory
func fileURL(name, dir, query string) string {
	return (&url.URL{Scheme: "file", Host: name, Path: filepath.ToSlash(dir), RawQuery: query}).String()
}

// status returns the HTTP status of an error from the client, or 0
func status(err error) int {
	var resp httpresponse.ErrResponse
	if errors.As(err, &resp) {
		return resp.Code
	}
	return 0
}

///////////////////////////////////////////////////////////////////////////////
// TESTS

func TestVolumes(t *testing.T) {
	client, tempDir := newClient(t)
	ctx := context.Background()
	dir := tempDir()

	// There are no volumes, and the schemes they can be created from
	list, err := client.ListVolumes(ctx)
	require.NoError(t, err)
	assert.Empty(t, list.Body)
	assert.Equal(t, []string{"file"}, list.Schemes)

	// A volume is created, with its options, and is listed
	volume, err := client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", dir, "readonly=true")})
	require.NoError(t, err)
	assert.Equal(t, "media", volume.Name)
	u, err := url.Parse(volume.URL)
	require.NoError(t, err)
	assert.Equal(t, "true", u.Query().Get("readonly"))
	list, err = client.ListVolumes(ctx)
	require.NoError(t, err)
	assert.Equal(t, []schema.Volume{*volume}, list.Body)

	// Errors are returned with their status
	_, err = client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", dir, "")})
	assert.Equal(t, http.StatusConflict, status(err), err)
	_, err = client.CreateVolume(ctx, schema.Volume{Name: "other", URL: "file://other"})
	assert.Equal(t, http.StatusBadRequest, status(err), err)
}

func TestListObjects(t *testing.T) {
	client, tempDir := newClient(t)
	ctx := context.Background()
	dir := tempDir()
	for _, name := range []string{"a.txt", "b/c.txt", "b/d.txt", "e.txt"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, filepath.Dir(name)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0o644))
	}
	_, err := client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", dir, "")})
	require.NoError(t, err)
	keys := func(list *schema.ObjectList) []string {
		result := []string{}
		for _, obj := range list.Body {
			assert.Equal(t, "media", obj.Name)
			result = append(result, obj.Key())
		}
		return result
	}
	req := func(path string, recursive bool, startAfter string, limit *uint64) schema.ObjectListRequest {
		var req schema.ObjectListRequest
		req.Name, req.Path, req.Recursive, req.StartAfter, req.Limit = "media", path, recursive, startAfter, limit
		return req
	}

	t.Run("Root", func(t *testing.T) {
		list, err := client.ListObjects(ctx, req("", false, "", nil))
		require.NoError(t, err)
		assert.Equal(t, []string{"/a.txt", "/b/", "/e.txt"}, keys(list))
	})

	t.Run("Path", func(t *testing.T) {
		list, err := client.ListObjects(ctx, req("/b", false, "", nil))
		require.NoError(t, err)
		assert.Equal(t, []string{"/b/c.txt", "/b/d.txt"}, keys(list))
	})

	t.Run("Pages", func(t *testing.T) {
		var all []string
		next := ""
		for range 10 {
			list, err := client.ListObjects(ctx, req("", true, next, types.Ptr(uint64(2))))
			require.NoError(t, err)
			all = append(all, keys(list)...)
			if next = list.Next; next == "" {
				break
			}
		}
		assert.Equal(t, []string{"/a.txt", "/b/", "/b/c.txt", "/b/d.txt", "/e.txt"}, all)
	})

	t.Run("Errors", func(t *testing.T) {
		_, err := client.ListObjects(ctx, schema.ObjectListRequest{ListObjectsRequest: schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Name: "missing"}}})
		assert.Equal(t, http.StatusNotFound, status(err), err)
		_, err = client.ListObjects(ctx, req("../etc", false, "", nil))
		assert.Equal(t, http.StatusBadRequest, status(err), err)
	})
}

func TestGetVolume(t *testing.T) {
	client, tempDir := newClient(t)
	ctx := context.Background()
	created, err := client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", tempDir(), "readonly=true")})
	require.NoError(t, err)

	// The volume is returned as it was created
	volume, err := client.GetVolume(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, created, volume)

	// A volume which isn't mounted isn't found
	_, err = client.GetVolume(ctx, "missing")
	assert.Equal(t, http.StatusNotFound, status(err), err)
}

func TestDeleteVolume(t *testing.T) {
	client, tempDir := newClient(t)
	ctx := context.Background()
	created, err := client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", tempDir(), "readonly=true")})
	require.NoError(t, err)

	// The volume is deleted, and returned as it was created, and isn't
	// listed
	deleted, err := client.DeleteVolume(ctx, "media")
	require.NoError(t, err)
	assert.Equal(t, created, deleted)
	list, err := client.ListVolumes(ctx)
	require.NoError(t, err)
	assert.Empty(t, list.Body)

	// A volume which isn't mounted isn't found
	_, err = client.DeleteVolume(ctx, "media")
	assert.Equal(t, http.StatusNotFound, status(err), err)
}

func TestUpdateVolume(t *testing.T) {
	client, tempDir := newClient(t)
	ctx := context.Background()
	_, err := client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", tempDir(), "readonly=true")})
	require.NoError(t, err)

	// An option is set, and one is removed
	volume, err := client.UpdateVolume(ctx, "media", schema.VolumePatch{"skip": json.RawMessage(`["\\.tmp$"]`), "readonly": json.RawMessage(`null`)})
	require.NoError(t, err)
	u, err := url.Parse(volume.URL)
	require.NoError(t, err)
	assert.Equal(t, url.Values{"skip": {`\.tmp$`}}, u.Query())

	// Errors are returned with their status
	_, err = client.UpdateVolume(ctx, "media", schema.VolumePatch{"unknown": json.RawMessage(`1`)})
	assert.Equal(t, http.StatusBadRequest, status(err), err)
	_, err = client.UpdateVolume(ctx, "missing", schema.VolumePatch{"readonly": json.RawMessage(`true`)})
	assert.Equal(t, http.StatusNotFound, status(err), err)
}

func TestHeadObject(t *testing.T) {
	client, tempDir := newClient(t)
	ctx := context.Background()
	dir := tempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "music"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "music", "a b.mp3"), []byte("abc"), 0o644))
	_, err := client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", dir, "")})
	require.NoError(t, err)

	// A file, with a name which is escaped, a directory, and the root
	object, err := client.HeadObject(ctx, "media", "/music/a b.mp3")
	require.NoError(t, err)
	assert.Equal(t, schema.ObjectKey{Name: "media", Path: "/music/a b.mp3"}, object.ObjectKey)
	assert.Equal(t, int64(3), *object.Size)
	assert.Equal(t, "audio/mpeg", object.ContentType)
	for _, path := range []string{"music", "/music/", "", "/"} {
		object, err := client.HeadObject(ctx, "media", path)
		require.NoError(t, err, path)
		assert.True(t, object.IsDir, path)
	}

	// Errors are returned with their status, and a path outside the volume
	// isn't requested
	_, err = client.HeadObject(ctx, "media", "/missing.txt")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	assert.EqualError(t, err, "not found: media/missing.txt")
	_, err = client.HeadObject(ctx, "media", "../etc")
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}

// statusCode returns the HTTP status of an error of a response, which has no
// body, such as a HEAD's, or 0
func statusCode(err error) int {
	var code httpresponse.Err
	if errors.As(err, &code) {
		return int(code)
	}
	return status(err)
}

func TestGetObject(t *testing.T) {
	client, tempDir := newClient(t)
	ctx := context.Background()
	dir := tempDir()
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "music"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "music", "a b.mp3"), []byte("abc"), 0o644))
	require.NoError(t, os.Chtimes(filepath.Join(dir, "music", "a b.mp3"), modtime, modtime))
	_, err := client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", dir, "")})
	require.NoError(t, err)
	key := func(path string) schema.GetObjectRequest {
		return schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Name: "media", Path: path}}
	}

	// The content is written, and the metadata returned
	var buf bytes.Buffer
	object, err := client.GetObject(ctx, key("/music/a b.mp3"), &buf)
	require.NoError(t, err)
	assert.Equal(t, "abc", buf.String())
	assert.Equal(t, "/music/a b.mp3", object.Path)
	assert.True(t, modtime.Equal(object.ModTime))

	// A file which isn't modified isn't written
	buf.Reset()
	req := key("/music/a b.mp3")
	req.IfModifiedSince = modtime
	_, err = client.GetObject(ctx, req, &buf)
	assert.ErrorIs(t, err, gofiler.ErrNotModified)
	assert.Empty(t, buf.String())

	// A directory can't be got, nor a file which doesn't exist
	for _, path := range []string{"/music", "/"} {
		_, err := client.GetObject(ctx, key(path), &buf)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, path)
	}
	_, err = client.GetObject(ctx, key("/missing.txt"), &buf)
	assert.Equal(t, http.StatusNotFound, status(err), err)
}

func TestDeleteObjects(t *testing.T) {
	client, tempDir := newClient(t)
	ctx := context.Background()
	dir := tempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "music", "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "music", "a b.mp3"), []byte("abc"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "music", "sub", "c.txt"), []byte("c"), 0o644))
	_, err := client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", dir, "")})
	require.NoError(t, err)
	req := func(path string, recursive bool) schema.DeleteObjectsRequest {
		return schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Name: "media", Path: path}, Recursive: recursive}
	}

	// A file, with a name which is escaped, and a directory, when it's
	// recursive
	deleted, err := client.DeleteObjects(ctx, req("/music/a b.mp3", false))
	require.NoError(t, err)
	require.Len(t, deleted.Body, 1)
	assert.Equal(t, schema.ObjectKey{Name: "media", Path: "/music/a b.mp3"}, deleted.Body[0].ObjectKey)
	_, err = client.DeleteObjects(ctx, req("/music", false))
	assert.Equal(t, http.StatusConflict, status(err), err)
	deleted, err = client.DeleteObjects(ctx, req("/music", true))
	require.NoError(t, err)
	require.Len(t, deleted.Body, 1)
	assert.Equal(t, "/music/sub/c.txt", deleted.Body[0].Path)

	// The root, and a path outside the volume, aren't requested
	for _, path := range []string{"", "/", "../etc"} {
		_, err := client.DeleteObjects(ctx, req(path, true))
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, path)
	}
	_, err = client.DeleteObjects(ctx, req("/missing.txt", false))
	assert.Equal(t, http.StatusNotFound, status(err), err)
}

func TestPutObject(t *testing.T) {
	client, tempDir := newClient(t)
	ctx := context.Background()
	dir := tempDir()
	_, err := client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", dir, "")})
	require.NoError(t, err)
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	req := func(path, body string) schema.CreateObjectRequest {
		return schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Name: "media", Path: path}, Body: strings.NewReader(body)}
	}

	// A file is created, with its modification time, and replaced
	r := req("/music/a b.mp3", "abc")
	r.ModTime = modtime
	object, err := client.PutObject(ctx, r)
	require.NoError(t, err)
	assert.Equal(t, schema.ObjectKey{Name: "media", Path: "/music/a b.mp3"}, object.ObjectKey)
	assert.True(t, modtime.Equal(object.ModTime))
	_, err = client.PutObject(ctx, req("/music/a b.mp3", "new"))
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(dir, "music", "a b.mp3"))
	require.NoError(t, err)
	assert.Equal(t, "new", string(data))

	// A nil body is an empty file
	object, err = client.PutObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Name: "media", Path: "/empty.txt"}})
	require.NoError(t, err)
	assert.Equal(t, int64(0), *object.Size)

	// Conditions which fail are gofiler.ErrPreconditionFailed
	r = req("/music/a b.mp3", "x")
	r.IfNotExists = true
	_, err = client.PutObject(ctx, r)
	assert.ErrorIs(t, err, gofiler.ErrPreconditionFailed)
	r = req("/missing.txt", "x")
	r.IfExists = true
	_, err = client.PutObject(ctx, r)
	assert.ErrorIs(t, err, gofiler.ErrPreconditionFailed)

	// The root, and a path outside the volume, aren't requested
	for _, path := range []string{"", "/", "../etc"} {
		_, err := client.PutObject(ctx, req(path, "x"))
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, path)
	}
	_, err = client.PutObject(ctx, req("/music", "x"))
	assert.Equal(t, http.StatusConflict, status(err), err)
}

func TestUploadObjects(t *testing.T) {
	client, tempDir := newClient(t)
	ctx := context.Background()
	dir := tempDir()
	_, err := client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", dir, "")})
	require.NoError(t, err)
	file := func(path, content string) httpclient.UploadFile {
		return httpclient.UploadFile{Path: path, Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(content)), nil }}
	}

	// Files are uploaded into the directory, with their directories, and
	// those which exist are skipped, unless they're overwritten
	resp, err := client.UploadObjects(ctx, "media", "/music", []httpclient.UploadFile{file("a.mp3", "a"), file("album/b.mp3", "b")}, false)
	require.NoError(t, err)
	require.Len(t, resp.Body, 2)
	assert.Equal(t, "/music/album/b.mp3", resp.Body[1].Path)
	resp, err = client.UploadObjects(ctx, "media", "/music", []httpclient.UploadFile{file("a.mp3", "new"), file(".hidden", "h")}, false)
	require.NoError(t, err)
	assert.Empty(t, resp.Body)
	require.Len(t, resp.Skipped, 2)
	assert.True(t, resp.Skipped[0].Exists)
	resp, err = client.UploadObjects(ctx, "media", "", []httpclient.UploadFile{file("music/a.mp3", "new")}, true)
	require.NoError(t, err)
	require.Len(t, resp.Body, 1)
	data, err := os.ReadFile(filepath.Join(dir, "music", "a.mp3"))
	require.NoError(t, err)
	assert.Equal(t, "new", string(data))

	// A file's modification time is kept
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	timed := file("timed.mp3", "t")
	timed.ModTime = modtime
	resp, err = client.UploadObjects(ctx, "media", "/music", []httpclient.UploadFile{timed}, false)
	require.NoError(t, err)
	require.Len(t, resp.Body, 1)
	assert.True(t, modtime.Equal(resp.Body[0].ModTime), resp.Body[0].ModTime)

	// A file which can't be opened stops the upload
	_, err = client.UploadObjects(ctx, "media", "/music", []httpclient.UploadFile{{Path: "x.mp3", Open: func() (io.ReadCloser, error) { return nil, os.ErrNotExist }}}, false)
	assert.Error(t, err)
	_, err = client.UploadObjects(ctx, "media", "../etc", []httpclient.UploadFile{file("a.mp3", "a")}, false)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}

func TestUploadObjectsNames(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows names can't have these characters")
	}
	client, tempDir := newClient(t)
	ctx := context.Background()
	dir := tempDir()
	_, err := client.CreateVolume(ctx, schema.Volume{Name: "media", URL: fileURL("media", dir, "")})
	require.NoError(t, err)

	// A name with characters which are quoted, or escaped, is the name
	for _, name := range []string{"a\tb.txt", `say "hi".txt`, "naïve café.txt", `back\slash/x.txt`} {
		name := strings.ReplaceAll(name, "\\t", "\t")
		resp, err := client.UploadObjects(ctx, "media", "/", []httpclient.UploadFile{{Path: name, Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("x")), nil }}}, false)
		require.NoError(t, err, name)
		require.Len(t, resp.Body, 1, name)
		assert.Equal(t, "/"+name, resp.Body[0].Path)
		_, err = os.Stat(filepath.Join(dir, filepath.FromSlash(name)))
		assert.NoError(t, err, name)
	}
}
