package httphandler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer/filer"
	httphandler "github.com/mutablelogic/go-filer/filer/httphandler"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// stalledWriter is a response to a client which stopped reading, so each
// write blocks until its deadline
type stalledWriter struct {
	sync.Mutex
	header   http.Header
	deadline time.Time
	blocked  chan struct{}
	once     sync.Once
}

func TestWatchAccept(t *testing.T) {
	server, _ := newServer(t)

	// Changes are only streamed, so another type isn't acceptable
	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/watch", nil)
	require.NoError(t, err)
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotAcceptable, resp.StatusCode)
}

func TestWatchStalled(t *testing.T) {
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

	// Watch, with a client which stops reading, so the keep-alive's write
	// blocks
	w := &stalledWriter{header: http.Header{}, blocked: make(chan struct{})}
	handled := make(chan struct{})
	go func() {
		defer close(handled)
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/watch", nil))
	}()
	select {
	case <-w.blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("nothing was written")
	}

	// Closing the registry interrupts the write, so the handler returns
	// soon, and not after the write's deadline
	closed := time.Now()
	require.NoError(t, registry.Close())
	select {
	case <-handled:
		assert.Less(t, time.Since(closed), 5*time.Second)
	case <-time.After(10 * time.Second):
		t.Fatal("the handler didn't return")
	}
}

func (w *stalledWriter) Header() http.Header {
	return w.header
}

func (w *stalledWriter) WriteHeader(int) {}

// Write blocks until the deadline, when one is set
func (w *stalledWriter) Write(data []byte) (int, error) {
	w.once.Do(func() { close(w.blocked) })
	for {
		w.Lock()
		deadline := w.deadline
		w.Unlock()
		if !deadline.IsZero() && time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (w *stalledWriter) FlushError() error {
	return nil
}

func (w *stalledWriter) SetWriteDeadline(deadline time.Time) error {
	w.Lock()
	defer w.Unlock()
	w.deadline = deadline
	return nil
}
