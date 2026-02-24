package httphandler_test

import (
	"context"
	"net/http"
	"os"
	"testing"

	// Packages
	httphandler "github.com/mutablelogic/go-filer/httphandler"
	manager "github.com/mutablelogic/go-filer/manager"
)

// serveMux creates an http.ServeMux with all httphandler routes registered.
func serveMux(mgr *manager.Manager) *http.ServeMux {
	mux := http.NewServeMux()
	path, handler, _ := httphandler.BackendListHandler(mgr)
	mux.HandleFunc(path, handler)
	path, handler, _ = httphandler.ObjectListHandler(mgr)
	mux.HandleFunc(path, handler)
	path, handler, _ = httphandler.ObjectHandler(mgr)
	mux.HandleFunc(path, handler)
	return mux
}

// newTestManager creates a manager with the given backend URLs.
func newTestManager(t *testing.T, backends ...string) *manager.Manager {
	t.Helper()
	opts := make([]manager.Opt, 0, len(backends))
	for _, b := range backends {
		opts = append(opts, manager.WithBackend(context.Background(), b))
	}
	mgr, err := manager.New(context.Background(), opts...)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	return mgr
}

// mustMkDir creates a directory or fails the test.
func mustMkDir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("failed to create directory %s: %v", path, err)
	}
}
