package httphandler_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
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

// newRegistryServer returns a server of the filer API, and its registry, with
// a volume "media", and the volume's directory
func newRegistryServer(t *testing.T) (*httptest.Server, *registry.Registry, string) {
	t.Helper()
	dir := t.TempDir()
	reg, err := registry.New(context.Background(), registry.WithStore(newStore(t)))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	running := make(chan error, 1)
	go func() { running <- reg.Run(ctx, nil) }()
	t.Cleanup(func() {
		cancel()
		<-running
	})
	mount(t, reg, "media", &url.URL{Scheme: "file", Host: "media", Path: filepath.ToSlash(dir)})
	mux := http.NewServeMux()
	router, err := httprouter.NewRouter(context.Background(), mux, "/api", "", "test", "0.0.0")
	require.NoError(t, err)
	require.NoError(t, httphandler.RegisterHandlers(reg, router))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, reg, dir
}

// removed deletes the volume, and fails the test when it isn't closed for
// long, which it is once a volume of the same name, which waits for it, is
// mounted
func removed(t *testing.T, reg *registry.Registry, dir string) {
	t.Helper()
	volume, err := reg.DeleteVolume(context.Background(), "media")
	require.NoError(t, err)

	// It's unmounted, which it is once it can't be used, and then a volume
	// of its name is mounted once it's closed
	require.Eventually(t, func() bool { return !slices.Contains(reg.Keys(), "media") }, 10*time.Second, time.Millisecond, "the volume isn't unmounted")
	_, err = reg.CreateVolume(context.Background(), schema.VolumeMeta{Name: "media", URL: volume.URL, Enabled: true})
	require.NoError(t, err)
	assert.Eventually(t, func() bool { return slices.Contains(reg.Keys(), "media") }, 10*time.Second, time.Millisecond, "removing the volume waited for a client which stalled")
}

func TestObjectInterruptUpload(t *testing.T) {
	server, reg, dir := newRegistryServer(t)

	// An upload whose client stalls, after part of its body, doesn't hold up
	// removing the volume
	body, w := io.Pipe()
	defer w.Close()
	req, err := http.NewRequest(http.MethodPut, server.URL+"/api/object/media/stalled.txt", body)
	require.NoError(t, err)
	req.ContentLength = 1 << 20
	go func() {
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	_, err = w.Write(make([]byte, 1024))
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	removed(t, reg, dir)
}

func TestObjectInterruptDownload(t *testing.T) {
	server, reg, dir := newRegistryServer(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "big.bin"), make([]byte, 64<<20), 0o644))

	// A download whose client doesn't read it, which stalls once the
	// connection's buffers are full, doesn't hold up removing the volume
	resp, err := http.Get(server.URL + "/api/object/media/big.bin")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	time.Sleep(200 * time.Millisecond)
	removed(t, reg, dir)
}
