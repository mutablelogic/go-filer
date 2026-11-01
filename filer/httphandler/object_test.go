package httphandler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

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
		for _, path := range []string{"/b", "b", "b/", "/x/../b"} {
			var list schema.ObjectList
			require.Equal(t, http.StatusOK, get(t, server, "object/media?path="+url.QueryEscape(path), &list), path)
			assert.Equal(t, []string{"/b/c.txt", "/b/d.txt"}, keys(list), path)
		}
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
