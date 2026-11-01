package httphandler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer/filer"
	httphandler "github.com/mutablelogic/go-filer/filer/httphandler"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// newServer returns a server of the filer API, for a registry with a volume
// "media", and the volume's directory
func newServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	dir := t.TempDir()
	registry, err := filer.New(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { registry.Close() })
	_, err = registry.Add(context.Background(), "media", &url.URL{Scheme: "file", Host: "media", Path: filepath.ToSlash(dir)})
	require.NoError(t, err)

	mux := http.NewServeMux()
	router, err := httprouter.NewRouter(context.Background(), mux, "/api", "", "test", "0.0.0")
	require.NoError(t, err)
	require.NoError(t, httphandler.RegisterHandlers(registry, router))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, dir
}

// get requests the path, and decodes the response into v when it's 200
func get(t *testing.T, server *httptest.Server, path string, v any) int {
	t.Helper()
	resp, err := http.Get(server.URL + "/api/" + path)
	require.NoError(t, err)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK && v != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(v))
	}
	return resp.StatusCode
}

// keys returns the keys of the objects of the list
func keys(list schema.ObjectList) []string {
	result := []string{}
	for _, obj := range list.Body {
		result = append(result, obj.Key())
	}
	return result
}

func TestObjectList(t *testing.T) {
	server, dir := newServer(t)
	for _, name := range []string{"a.txt", "b/c.txt", "b/d.txt", ".hidden", "e.txt"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, filepath.Dir(name)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0o644))
	}

	t.Run("Root", func(t *testing.T) {
		var list schema.ObjectList
		require.Equal(t, http.StatusOK, get(t, server, "object/media", &list))
		assert.Equal(t, []string{"/a.txt", "/b/", "/e.txt"}, keys(list))
		assert.Empty(t, list.Next)
		for _, obj := range list.Body {
			assert.Equal(t, "media", obj.Name)
		}
	})

	t.Run("Path", func(t *testing.T) {
		for _, path := range []string{"/b", "b", "b/", "/x/../b", "/x/../b/"} {
			var list schema.ObjectList
			require.Equal(t, http.StatusOK, get(t, server, "object/media?path="+url.QueryEscape(path), &list), path)
			assert.Equal(t, []string{"/b/c.txt", "/b/d.txt"}, keys(list), path)
		}

		// A path which ends with "/" is a directory, which a file isn't
		assert.Equal(t, http.StatusNotFound, get(t, server, "object/media?path="+url.QueryEscape("/a.txt/"), nil))
	})

	t.Run("Recursive", func(t *testing.T) {
		var list schema.ObjectList
		require.Equal(t, http.StatusOK, get(t, server, "object/media?recursive=true", &list))
		assert.Equal(t, []string{"/a.txt", "/b/", "/b/c.txt", "/b/d.txt", "/e.txt"}, keys(list))
	})

	t.Run("Pages", func(t *testing.T) {
		// Each page starts after the last key of the previous one, until
		// there's no next page
		var all []string
		next := ""
		for page := 0; page < 10; page++ {
			var list schema.ObjectList
			require.Equal(t, http.StatusOK, get(t, server, fmt.Sprintf("object/media?recursive=true&limit=2&start_after=%s", url.QueryEscape(next)), &list))
			assert.LessOrEqual(t, len(list.Body), 2)
			all = append(all, keys(list)...)
			if next = list.Next; next == "" {
				break
			}
			assert.Equal(t, list.Body[len(list.Body)-1].Key(), next)
		}
		assert.Equal(t, []string{"/a.txt", "/b/", "/b/c.txt", "/b/d.txt", "/e.txt"}, all)
	})

	t.Run("Limit", func(t *testing.T) {
		// A limit of 0 is 1
		var list schema.ObjectList
		require.Equal(t, http.StatusOK, get(t, server, "object/media?limit=0", &list))
		assert.Len(t, list.Body, 1)
		assert.Equal(t, "/a.txt", list.Next)
	})

	t.Run("Errors", func(t *testing.T) {
		for path, want := range map[string]int{
			"object/missing":              http.StatusNotFound,
			"object/media?path=/missing":  http.StatusNotFound,
			"object/media?path=../etc":    http.StatusBadRequest,
			"object/media?path=/a/../../": http.StatusBadRequest,
			"object/media?limit=abc":      http.StatusBadRequest,
			"object/media?recursive=what": http.StatusBadRequest,
		} {
			assert.Equal(t, want, get(t, server, path, nil), path)
		}
	})
}

// head requests the path with HEAD, and returns its status and headers
func head(t *testing.T, server *httptest.Server, path string, header http.Header) (int, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodHead, server.URL+"/api/"+path, nil)
	require.NoError(t, err)
	for key, values := range header {
		req.Header[key] = values
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode, resp.Header
}

func TestObjectHead(t *testing.T) {
	server, dir := newServer(t)
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "music"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "music", "a b.mp3"), []byte("abc"), 0o644))
	require.NoError(t, os.Chtimes(filepath.Join(dir, "music", "a b.mp3"), modtime, modtime))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".hidden"), nil, 0o644))
	meta := func(header http.Header) schema.Object {
		var object schema.Object
		require.NoError(t, json.Unmarshal([]byte(header.Get("X-Meta")), &object))
		return object
	}

	// A file has its type, size and modification time, and its metadata
	status, header := head(t, server, "object/media/music/a%20b.mp3", nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "audio/mpeg", header.Get("Content-Type"))
	assert.Equal(t, "3", header.Get("Content-Length"))
	assert.Equal(t, modtime.Format(http.TimeFormat), header.Get("Last-Modified"))
	object := meta(header)
	assert.Equal(t, "media", object.Name)
	assert.Equal(t, "/music/a b.mp3", object.Path)
	assert.Equal(t, int64(3), *object.Size)

	// A directory, and the root, are directories. A path with ".." isn't
	// tried, as the router cleans it, and redirects to the cleaned path.
	for _, path := range []string{"object/media/music", "object/media/music/", "object/media/"} {
		status, header := head(t, server, path, nil)
		require.Equal(t, http.StatusOK, status, path)
		assert.Equal(t, schema.DirectoryType, header.Get("Content-Type"), path)
		assert.True(t, meta(header).IsDir, path)
	}

	// A file which isn't modified isn't
	status, _ = head(t, server, "object/media/music/a%20b.mp3", http.Header{"If-Modified-Since": {modtime.Format(http.TimeFormat)}})
	assert.Equal(t, http.StatusNotModified, status)
	status, _ = head(t, server, "object/media/music/a%20b.mp3", http.Header{"If-Modified-Since": {modtime.Add(-time.Hour).Format(http.TimeFormat)}})
	assert.Equal(t, http.StatusOK, status)
	status, _ = head(t, server, "object/media/music/a%20b.mp3", http.Header{"If-Unmodified-Since": {modtime.Add(-time.Hour).Format(http.TimeFormat)}})
	assert.Equal(t, http.StatusPreconditionFailed, status)

	// Errors
	for path, want := range map[string]int{
		"object/media/missing.txt":   http.StatusNotFound,
		"object/media/.hidden":       http.StatusNotFound,
		"object/missing/a.txt":       http.StatusNotFound,
		"object/media/music%2Fa.mp3": http.StatusBadRequest,
		"object/media/%2E%2E/etc":    http.StatusBadRequest,
	} {
		status, _ := head(t, server, path, nil)
		assert.Equal(t, want, status, path)
	}
}

// getObject requests the path, without following a redirect, and returns
// its status, headers and body
func getObject(t *testing.T, server *httptest.Server, path string, header http.Header) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/"+path, nil)
	require.NoError(t, err)
	for key, values := range header {
		req.Header[key] = values
	}
	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, resp.Header, string(body)
}

func TestObjectGet(t *testing.T) {
	server, dir := newServer(t)
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "music", "Disk.sparsebundle"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "music", "a b.mp3"), []byte("abc"), 0o644))
	require.NoError(t, os.Chtimes(filepath.Join(dir, "music", "a b.mp3"), modtime, modtime))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "page.html"), []byte("<script>alert(1)</script>"), 0o644))

	// A file's content, with its headers, and metadata, shown in the
	// browser, or downloaded
	status, header, body := getObject(t, server, "object/media/music/a%20b.mp3", nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "abc", body)
	assert.Equal(t, "audio/mpeg", header.Get("Content-Type"))
	assert.Equal(t, "3", header.Get("Content-Length"))
	assert.Equal(t, modtime.Format(http.TimeFormat), header.Get("Last-Modified"))
	assert.Contains(t, header.Get("Content-Disposition"), "inline")
	assert.Contains(t, header.Get("X-Meta"), `"path":"/music/a b.mp3"`)
	_, header, _ = getObject(t, server, "object/media/music/a%20b.mp3?download=true", nil)
	assert.Contains(t, header.Get("Content-Disposition"), "attachment")

	// A file on disk can seek, so it's read in ranges
	status, header, body = getObject(t, server, "object/media/music/a%20b.mp3", http.Header{"Range": {"bytes=1-"}})
	require.Equal(t, http.StatusPartialContent, status)
	assert.Equal(t, "bc", body)
	assert.Equal(t, "bytes 1-2/3", header.Get("Content-Range"))
	status, _, _ = getObject(t, server, "object/media/music/a%20b.mp3", http.Header{"Range": {"bytes=10-"}})
	assert.Equal(t, http.StatusRequestedRangeNotSatisfiable, status)
	_, header, _ = getObject(t, server, "object/media/music/a%20b.mp3", nil)
	assert.Equal(t, "bytes", header.Get("Accept-Ranges"))

	// HTML is sandboxed, and isn't sniffed
	_, header, _ = getObject(t, server, "object/media/page.html", nil)
	assert.Contains(t, header.Get("Content-Security-Policy"), "sandbox")
	assert.Equal(t, "nosniff", header.Get("X-Content-Type-Options"))

	// Conditions
	status, _, body = getObject(t, server, "object/media/music/a%20b.mp3", http.Header{"If-Modified-Since": {modtime.Format(http.TimeFormat)}})
	assert.Equal(t, http.StatusNotModified, status)
	assert.Empty(t, body)
	status, _, _ = getObject(t, server, "object/media/music/a%20b.mp3", http.Header{"If-None-Match": {"*"}})
	assert.Equal(t, http.StatusNotModified, status)
	status, _, _ = getObject(t, server, "object/media/music/a%20b.mp3", http.Header{"If-Match": {`"other"`}})
	assert.Equal(t, http.StatusPreconditionFailed, status)
	status, _, body = getObject(t, server, "object/media/music/a%20b.mp3", http.Header{"If-Match": {"*"}})
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "abc", body)
	status, _, _ = getObject(t, server, "object/media/music/a%20b.mp3", http.Header{"If-Unmodified-Since": {modtime.Add(-time.Hour).Format(http.TimeFormat)}})
	assert.Equal(t, http.StatusPreconditionFailed, status)

	// A directory, or the root, redirects to its listing
	status, header, _ = getObject(t, server, "object/media/music", nil)
	assert.Equal(t, http.StatusTemporaryRedirect, status)
	assert.Equal(t, "/api/object/media?path=%2Fmusic%2F", header.Get("Location"))
	status, header, _ = getObject(t, server, "object/media/", nil)
	assert.Equal(t, http.StatusTemporaryRedirect, status)
	assert.Equal(t, "/api/object/media", header.Get("Location"))

	// Errors
	for path, want := range map[string]int{
		"object/media/missing.txt":                 http.StatusNotFound,
		"object/missing/a.txt":                     http.StatusNotFound,
		"object/media/music%2Fa%20b.mp3":           http.StatusBadRequest,
		"object/media/music/a%20b.mp3?download=no": http.StatusBadRequest,
		"object/media/music/Disk.sparsebundle":     http.StatusNotImplemented,
	} {
		status, _, _ := getObject(t, server, path, nil)
		assert.Equal(t, want, status, path)
	}
}

func TestObjectDelete(t *testing.T) {
	server, dir := newServer(t)
	for _, name := range []string{"a.txt", "music/b.mp3", "music/sub/c.txt", "music/.hidden", "keep/d.txt"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, filepath.Dir(name)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0o644))
	}
	exists := func(name string) bool {
		_, err := os.Stat(filepath.Join(dir, filepath.FromSlash(name)))
		return err == nil
	}
	paths := func(resp schema.DeleteObjectsResponse) []string {
		result := []string{}
		for _, obj := range resp.Body {
			assert.Equal(t, "media", obj.Name)
			result = append(result, obj.Path)
		}
		return result
	}

	// A file is deleted, and returned
	var resp schema.DeleteObjectsResponse
	require.Equal(t, http.StatusOK, del(t, server, "object/media/a.txt", &resp))
	assert.Equal(t, []string{"/a.txt"}, paths(resp))
	assert.False(t, exists("a.txt"))

	// A directory isn't, unless it's recursive, when every object beneath it
	// is, including hidden files, and empty parents are removed
	assert.Equal(t, http.StatusConflict, del(t, server, "object/media/music", nil))
	assert.True(t, exists("music/b.mp3"))
	require.Equal(t, http.StatusOK, del(t, server, "object/media/music?recursive=true", &resp))
	assert.ElementsMatch(t, []string{"/music/b.mp3", "/music/sub/c.txt", "/music/.hidden"}, paths(resp))
	assert.False(t, exists("music"))
	assert.True(t, exists("keep/d.txt"))

	// Errors
	for path, want := range map[string]int{
		"object/media/missing.txt":          http.StatusNotFound,
		"object/missing/a.txt":              http.StatusNotFound,
		"object/media/":                     http.StatusBadRequest,
		"object/media/keep%2Fd.txt":         http.StatusBadRequest,
		"object/media/keep?recursive=maybe": http.StatusBadRequest,
	} {
		assert.Equal(t, want, del(t, server, path, nil), path)
	}
	assert.True(t, exists("keep/d.txt"))

	// A volume which is read-only can't be deleted from
	require.Equal(t, http.StatusOK, patch(t, server, "volume/media", `{"readonly": true}`, nil))
	assert.Equal(t, http.StatusForbidden, del(t, server, "object/media/keep/d.txt", nil))
	assert.True(t, exists("keep/d.txt"))
}

// put requests the path with PUT and the body, with the headers, and
// decodes the response into v when it's 200 or 201
func put(t *testing.T, server *httptest.Server, path string, body io.Reader, header http.Header, v any) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, server.URL+"/api/"+path, body)
	require.NoError(t, err)
	for key, values := range header {
		req.Header[key] = values
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	if (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated) && v != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(v))
	}
	return resp.StatusCode
}

func TestObjectPut(t *testing.T) {
	server, dir := newServer(t)
	content := func(name string) string {
		data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			return "<missing>"
		}
		return string(data)
	}
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

	// A file is created, with its directories, and modification time, and
	// replaced
	var object schema.Object
	require.Equal(t, http.StatusCreated, put(t, server, "object/media/music/a%20b.mp3", strings.NewReader("abc"), http.Header{"Last-Modified": {modtime.Format(http.TimeFormat)}}, &object))
	assert.Equal(t, schema.ObjectKey{Name: "media", Path: "/music/a b.mp3"}, object.ObjectKey)
	assert.Equal(t, int64(3), *object.Size)
	assert.True(t, modtime.Equal(object.ModTime))
	assert.Equal(t, "abc", content("music/a b.mp3"))
	require.Equal(t, http.StatusOK, put(t, server, "object/media/music/a%20b.mp3", strings.NewReader("new"), http.Header{"Content-Type": {"application/octet-stream"}}, &object))
	assert.Equal(t, "new", content("music/a b.mp3"))

	// Conditions
	assert.Equal(t, http.StatusPreconditionFailed, put(t, server, "object/media/music/a%20b.mp3", strings.NewReader("x"), http.Header{"If-None-Match": {"*"}}, nil))
	assert.Equal(t, http.StatusPreconditionFailed, put(t, server, "object/media/missing.txt", strings.NewReader("x"), http.Header{"If-Match": {"*"}}, nil))
	assert.Equal(t, "new", content("music/a b.mp3"))
	assert.Equal(t, "<missing>", content("missing.txt"))
	require.Equal(t, http.StatusCreated, put(t, server, "object/media/c.txt", strings.NewReader("c"), http.Header{"If-None-Match": {"*"}}, nil))
	require.Equal(t, http.StatusOK, put(t, server, "object/media/c.txt", strings.NewReader("d"), http.Header{"If-Match": {"*"}}, nil))
	assert.Equal(t, "d", content("c.txt"))

	// Errors
	for path, test := range map[string]struct {
		header http.Header
		want   int
	}{
		"object/media/music":     {nil, http.StatusConflict},
		"object/media/":          {nil, http.StatusBadRequest},
		"object/media/.hidden":   {nil, http.StatusUnprocessableEntity},
		"object/media/a%2Fb.txt": {nil, http.StatusBadRequest},
		"object/missing/a.txt":   {nil, http.StatusNotFound},
		"object/media/e.txt":     {http.Header{"If-None-Match": {`"etag"`}}, http.StatusBadRequest},
		"object/media/f.txt":     {http.Header{"Last-Modified": {"yesterday"}}, http.StatusBadRequest},
		"object/media/g.txt":     {http.Header{"Content-Type": {"image/png"}}, http.StatusNotImplemented},
	} {
		assert.Equal(t, test.want, put(t, server, path, strings.NewReader("x"), test.header, nil), path)
	}

	// A body which ends before its length doesn't change the file
	req, err := http.NewRequest(http.MethodPut, server.URL+"/api/object/media/c.txt", io.LimitReader(strings.NewReader("abcdef"), 3))
	require.NoError(t, err)
	req.ContentLength = 6
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	}
	assert.Equal(t, "d", content("c.txt"))

	// A volume which is read-only can't be uploaded to
	require.Equal(t, http.StatusOK, patch(t, server, "volume/media", `{"readonly": true}`, nil))
	assert.Equal(t, http.StatusForbidden, put(t, server, "object/media/h.txt", strings.NewReader("x"), nil, nil))
}

// upload posts the files, by their names, as a multipart form, to the path,
// and decodes the response into v when it's 200 or 201
func upload(t *testing.T, server *httptest.Server, path string, files [][2]string, v any) int {
	t.Helper()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	for _, file := range files {
		header := textproto.MIMEHeader{}
		header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, file[0]))
		header.Set("Content-Type", "application/octet-stream")
		w, err := form.CreatePart(header)
		require.NoError(t, err)
		_, err = w.Write([]byte(file[1]))
		require.NoError(t, err)
	}
	require.NoError(t, form.WriteField("other", "ignored"))
	require.NoError(t, form.Close())
	resp, err := http.Post(server.URL+"/api/"+path, form.FormDataContentType(), &body)
	require.NoError(t, err)
	defer resp.Body.Close()
	if (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated) && v != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(v))
	}
	return resp.StatusCode
}

func TestObjectUpload(t *testing.T) {
	server, dir := newServer(t)
	content := func(name string) string {
		data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			return "<missing>"
		}
		return string(data)
	}
	paths := func(resp schema.ObjectUploadResponse) ([]string, map[string]bool) {
		written, skipped := []string{}, map[string]bool{}
		for _, obj := range resp.Body {
			assert.Equal(t, "media", obj.Name)
			written = append(written, obj.Path)
		}
		for _, skip := range resp.Skipped {
			skipped[skip.Path] = skip.Exists
		}
		return written, skipped
	}

	// Files are written into the directory, which is created, with the
	// directories in their names
	var resp schema.ObjectUploadResponse
	require.Equal(t, http.StatusCreated, upload(t, server, "object/media/music", [][2]string{{"a.mp3", "a"}, {"album/b.mp3", "b"}, {`album\c.mp3`, "c"}}, &resp))
	written, skipped := paths(resp)
	assert.Equal(t, []string{"/music/a.mp3", "/music/album/b.mp3", "/music/album/c.mp3"}, written)
	assert.Empty(t, skipped)
	assert.Equal(t, "b", content("music/album/b.mp3"))
	assert.Equal(t, "c", content("music/album/c.mp3"))

	// Files which exist are skipped, unless they're overwritten, as are
	// those which can't be stored, or are outside the directory
	require.Equal(t, http.StatusCreated, upload(t, server, "object/media/music", [][2]string{{"a.mp3", "new"}, {"d.mp3", "d"}, {".hidden", "h"}, {"album", "x"}, {"../escape.txt", "e"}, {"", "x"}}, &resp))
	written, skipped = paths(resp)
	assert.Equal(t, []string{"/music/d.mp3"}, written)
	assert.Equal(t, map[string]bool{"/music/a.mp3": true, "/music/.hidden": false, "/music/album": false, "/escape.txt": false, "/music": false}, skipped)
	assert.Equal(t, "a", content("music/a.mp3"))
	assert.Equal(t, "<missing>", content("escape.txt"))
	require.Equal(t, http.StatusCreated, upload(t, server, "object/media/music?overwrite=true", [][2]string{{"a.mp3", "new"}}, &resp))
	assert.Equal(t, "new", content("music/a.mp3"))

	// A name with "/" keeps its "\", which is a character of a name, except
	// on Windows, whose names can't have it
	if runtime.GOOS != "windows" {
		require.Equal(t, http.StatusCreated, upload(t, server, "object/media/music", [][2]string{{`album/x\y.mp3`, "xy"}}, &resp))
		assert.Equal(t, "xy", content(`music/album/x\y.mp3`))
	}

	// Every file which is skipped is 200, and the root is a directory
	require.Equal(t, http.StatusOK, upload(t, server, "object/media/music", [][2]string{{"a.mp3", "x"}}, &resp))
	require.Equal(t, http.StatusCreated, upload(t, server, "object/media/", [][2]string{{"root.txt", "r"}}, &resp))
	assert.Equal(t, "r", content("root.txt"))

	// Errors
	assert.Equal(t, http.StatusConflict, upload(t, server, "object/media/root.txt", [][2]string{{"a.txt", "x"}}, nil))
	assert.Equal(t, http.StatusNotFound, upload(t, server, "object/missing/", [][2]string{{"a.txt", "x"}}, nil))
	resp2, err := http.Post(server.URL+"/api/object/media/music", "text/plain", strings.NewReader("not a form"))
	require.NoError(t, err)
	resp2.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp2.StatusCode)
	require.Equal(t, http.StatusOK, patch(t, server, "volume/media", `{"readonly": true}`, nil))
	assert.Equal(t, http.StatusForbidden, upload(t, server, "object/media/music", [][2]string{{"e.mp3", "x"}}, nil))
}
