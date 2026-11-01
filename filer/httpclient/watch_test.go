package httpclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/filer"
	fsevents "github.com/mutablelogic/go-filer/filer/fs/fsevents"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	httphandler "github.com/mutablelogic/go-filer/filer/httphandler"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// watch watches the volumes, until the test ends, and returns the changes,
// and the error watching returns
func watch(t *testing.T, client *httpclient.Client, names ...string) (chan schema.Event, chan error) {
	t.Helper()
	events, done := make(chan schema.Event, 1024), make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		done <- client.WatchObjects(ctx, names, func(event schema.Event) error {
			events <- event
			return nil
		})
	}()
	time.Sleep(200 * time.Millisecond)
	return events, done
}

// waitFor calls the function, which changes an object, until a change which
// matches is received, or fails the test, as a volume which is added is
// watched once its watching starts, which it isn't, straight away
func waitFor(t *testing.T, events chan schema.Event, change func(), match func(schema.Event) bool) {
	t.Helper()
	for deadline := time.After(10 * time.Second); ; {
		change()
		select {
		case event := <-events:
			if match(event) {
				return
			}
		case <-time.After(200 * time.Millisecond):
		case <-deadline:
			t.Fatal("no change which matches")
		}
	}
}

func TestWatchObjects(t *testing.T) {
	if !fsevents.Supported() {
		t.Skip(fsevents.ErrNotSupported)
	}
	client, tempDir := newClient(t)
	ctx := context.Background()
	for _, name := range []string{"a", "b"} {
		_, err := client.CreateVolume(ctx, schema.Volume{Name: name, URL: fileURL(name, tempDir(), "")})
		require.NoError(t, err)
	}
	put := func(name, path string) {
		_, err := client.PutObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Name: name, Path: path}, Body: strings.NewReader("x")})
		require.NoError(t, err)
	}

	// The changes to every volume, and to those which are watched, with the
	// names of their volumes
	all, _ := watch(t, client)
	onlyB, _ := watch(t, client, "b")
	waitFor(t, all, func() { put("a", "/a.txt") }, func(event schema.Event) bool { return event.Name == "a" && event.Path == "/a.txt" })
	waitFor(t, all, func() { put("b", "/b.txt") }, func(event schema.Event) bool { return event.Name == "b" && event.Path == "/b.txt" })
	waitFor(t, onlyB, func() { put("b", "/b.txt") }, func(event schema.Event) bool {
		require.Equal(t, "b", event.Name, "a change to a volume which isn't watched")
		return event.Path == "/b.txt"
	})
}

func TestWatchObjectsShutdown(t *testing.T) {
	registry, err := filer.New(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { registry.Close() })
	_, err = registry.Add(context.Background(), "media", fileURLParse(t, t.TempDir()))
	require.NoError(t, err)
	mux := http.NewServeMux()
	router, err := httprouter.NewRouter(context.Background(), mux, "/api", "", "test", "0.0.0")
	require.NoError(t, err)
	require.NoError(t, httphandler.RegisterHandlers(registry, router))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client, err := httpclient.New(server.URL + "/api")
	require.NoError(t, err)

	// Closing the registry, as the server is stopping, ends the stream
	_, done := watch(t, client)
	require.NoError(t, registry.Close())
	select {
	case err := <-done:
		assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
	case <-time.After(10 * time.Second):
		t.Fatal("the stream didn't end when the registry was closed")
	}
}

// fileURLParse returns the URL of a volume "media" for the directory
func fileURLParse(t *testing.T, dir string) *url.URL {
	t.Helper()
	u, err := url.Parse(fileURL("media", dir, ""))
	require.NoError(t, err)
	return u
}
