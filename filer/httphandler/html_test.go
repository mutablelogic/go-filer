package httphandler_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	// Packages
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// getAccept requests the path, accepting the media types, and returns its
// status, type and body
func getAccept(t *testing.T, server *httptest.Server, path, accept string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/"+path, nil)
	require.NoError(t, err)
	req.Header.Set("Accept", accept)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, resp.Header.Values("Vary"), "Accept")
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(body)
}

func TestObjectListHTML(t *testing.T) {
	server, dir := newServer(t)
	// A name with characters which are escaped in HTML, which Windows allows
	for _, name := range []string{"a.txt", "music/b.mp3", "a&b'c.txt"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, filepath.Dir(name)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(name), 0o644))
	}
	browser := "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

	// A browser is sent HTML, with the objects, whose names are escaped, and
	// links to the directories, and of the path
	status, contentType, body := getAccept(t, server, "object/media", browser)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "text/html; charset=utf-8", contentType)
	assert.Contains(t, body, "a.txt")
	assert.Contains(t, body, "a&amp;b&#39;c.txt")
	assert.NotContains(t, body, "a&b'c.txt")
	assert.Contains(t, body, `<a href="?path=%2Fmusic">music/</a>`)
	assert.Contains(t, body, `<a href="?">media</a>`)
	assert.Contains(t, body, `<a href="media/a.txt">a.txt</a>`)
	assert.Contains(t, body, `<a href="media/a&amp;b%27c.txt">a&amp;b&#39;c.txt</a>`)

	// Each file has a link which downloads it, and a directory doesn't
	assert.Contains(t, body, `<a class="download" href="media/a.txt?download=true" title="Download a.txt"`)
	assert.Contains(t, body, `href="media/a&amp;b%27c.txt?download=true" title="Download a&amp;b&#39;c.txt"`)
	assert.NotContains(t, body, `music?download=true`)

	// Each file, and directory, can be deleted, a directory with every
	// object beneath it, with a script which asks to confirm
	assert.Contains(t, body, `<button class="delete" type="button" data-href="media/a.txt" data-name="a.txt"`)
	assert.Contains(t, body, `data-href="media/music?recursive=true" data-name="music/"`)
	assert.Contains(t, body, `method: "DELETE"`)

	// A directory's names are relative to it, and the path links back
	_, _, body = getAccept(t, server, "object/media?path=/music", browser)
	assert.Contains(t, body, ">b.mp3<")
	assert.Contains(t, body, `<a href="?path=%2Fmusic">music</a>`)

	// A page with more objects links to the next, keeping the query
	_, _, body = getAccept(t, server, "object/media?limit=1&recursive=true", browser)
	assert.Contains(t, body, "Next page")
	assert.Contains(t, body, `limit=1&amp;recursive=true&amp;start_after=%2Fa%26b%27c.txt`)

	// JSON is sent otherwise, and when it's accepted before HTML
	for _, accept := range []string{"", "application/json", "*/*", "application/json, text/html"} {
		status, contentType, body := getAccept(t, server, "object/media", accept)
		require.Equal(t, http.StatusOK, status, accept)
		assert.True(t, strings.HasPrefix(contentType, "application/json"), accept)
		assert.True(t, strings.HasPrefix(body, "{"), accept)
	}

	// Files can be uploaded into the directory, or the root
	assert.Contains(t, body, `<input type="file" multiple data-href="media/">`)
	_, _, body = getAccept(t, server, "object/media?path=/music", browser)
	assert.Contains(t, body, `<input type="file" multiple data-href="media/music/">`)
	assert.Contains(t, body, `method: "POST"`)

	// A volume which is read-only, by any boolean, has nothing to upload
	// or delete with
	require.Equal(t, http.StatusOK, patch(t, server, "volume/media", `{"readonly": "TRUE"}`, nil))
	_, _, body = getAccept(t, server, "object/media", browser)
	assert.Contains(t, body, "a.txt")
	assert.NotContains(t, body, `class="delete"`)
	assert.NotContains(t, body, `type="file"`)
	assert.NotContains(t, body, "<script>")

	// Errors are as they are for JSON
	status, _, _ = getAccept(t, server, "object/missing", browser)
	assert.Equal(t, http.StatusNotFound, status)
}
