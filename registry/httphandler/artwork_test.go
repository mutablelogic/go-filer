package httphandler_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	// Packages
	registry "github.com/mutablelogic/go-filer/registry"
	httphandler "github.com/mutablelogic/go-filer/registry/httphandler"
	schema "github.com/mutablelogic/go-filer/schema"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_Artwork(t *testing.T) {
	ctx := context.Background()

	// A server of a volume, whose file, once it's indexed, links to artwork
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.mp3"), []byte("mp3"), 0o644))
	s := newStore(t)
	reg, err := registry.New(ctx, registry.WithStore(s))
	require.NoError(t, err)
	rctx, cancel := context.WithCancel(ctx)
	running := make(chan error, 1)
	go func() { running <- reg.Run(rctx, nil) }()
	t.Cleanup(func() {
		cancel()
		<-running
	})
	mount(t, reg, "media", &url.URL{Scheme: "file", Host: "media", Path: filepath.ToSlash(dir), RawQuery: "index=objects"})
	require.Eventually(t, func() bool {
		_, err := s.GetObject(ctx, "media", "/a.mp3")
		return err == nil
	}, 10*time.Second, time.Millisecond, "the file isn't indexed")
	data := []byte("\x89PNG\r\n\x1a\ncover")
	linked, err := s.SetObjectArtwork(ctx, "media", "/a.mp3", schema.ArtworkMeta{Type: "image/png", Width: 2, Height: 1, Context: schema.ArtworkCover, Data: data})
	require.NoError(t, err)
	require.Len(t, linked, 1)
	hash := linked[0].Hash
	mux := http.NewServeMux()
	router, err := httprouter.NewRouter(ctx, mux, "/api", "", "test", "0.0.0")
	require.NoError(t, err)
	require.NoError(t, httphandler.RegisterHandlers(reg, router))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	get := func(method, hash, match string) (*http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+"/api/artwork/"+hash, nil)
		require.NoError(t, err)
		if match != "" {
			req.Header.Set("If-None-Match", match)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp, body
	}

	// The image, of its type, whose entity tag is its hash, which a browser
	// keeps, as it never changes
	resp, body := get(http.MethodGet, hash, "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, data, body)
	assert.Equal(t, "image/png", resp.Header.Get("Content-Type"))
	assert.Equal(t, `"`+hash+`"`, resp.Header.Get("ETag"))
	assert.Equal(t, "private, max-age=31536000, immutable", resp.Header.Get("Cache-Control"))
	assert.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))

	// Its headers alone, of a HEAD request, and none, when it's kept
	resp, body = get(http.MethodHead, hash, "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, body)
	assert.Equal(t, strconv.Itoa(len(data)), resp.Header.Get("Content-Length"))
	resp, body = get(http.MethodGet, hash, `W/"`+hash+`"`)
	assert.Equal(t, http.StatusNotModified, resp.StatusCode)
	assert.Empty(t, body)

	// A hash which isn't one, or of no artwork
	resp, _ = get(http.MethodGet, "cover", "")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp, _ = get(http.MethodGet, "0000000000000000000000000000000000000000000000000000000000000000", "")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
