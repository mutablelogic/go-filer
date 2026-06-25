package test

import (
	"net/url"
	"testing"

	// Packages
	backend "github.com/mutablelogic/go-filer/backend"
	registry "github.com/mutablelogic/go-filer/backend/registry"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	r *registry.Registry
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Main is the test main function for backend tests.
func Main(m *testing.M) {
	// Create a new registry
	r = registry.New()
	if r == nil {
		panic("failed to create registry")
	}
	m.Run()
}

func BeginFile(t *testing.T) backend.Backend {
	t.Helper()
	if r == nil {
		t.Fatal("registry is not initialized")
	}
	tempdir := t.TempDir()
	url, err := url.Parse("file://" + t.Name() + tempdir)
	if err != nil {
		t.Fatalf("failed to parse URL: %v", err)
	}
	if _, err := r.Validate(url); err != nil {
		t.Fatalf("failed to validate URL: %v", err)
	}
	backend, err := r.New(url.String())
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	if backend == nil {
		t.Fatalf("failed to get backend")
	}
	return backend
}

// End releases the per-test context created by Begin.
func End(t *testing.T, backend backend.Backend) {
	t.Helper()
	if err := backend.Close(); err != nil {
		t.Errorf("failed to close backend: %v", err)
	}
}
