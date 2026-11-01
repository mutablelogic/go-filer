package httpclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	// Packages
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
