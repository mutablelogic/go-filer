package httphandler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

func Test_backendList(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/filer", nil)
	rw := httptest.NewRecorder()

	// Create a minimal manager with no backends (empty manager)
	manager, err := filer.New(context.Background())
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	err = backendList(rw, req, manager)
	if err != nil {
		t.Fatalf("backendList returned error: %v", err)
	}

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var out schema.ListResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// For an empty manager, we should get an empty list
	if len(out.Body) != 0 {
		t.Errorf("expected empty backends list, got %+v", out.Body)
	}
}

func Test_backendList_withBackends(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/filer", nil)
	rw := httptest.NewRecorder()

	// Create temporary directories for file backends
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	backupPath := tempDir + "/backup"

	// Create the directories
	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		t.Fatalf("failed to create backup directory: %v", err)
	}

	// Create a manager with some file backends
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
		filer.WithFileBackend("backup", backupPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	err = backendList(rw, req, manager)
	if err != nil {
		t.Fatalf("backendList returned error: %v", err)
	}

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var out schema.BackendListResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Should get 2 backends
	if len(out.Body) != 2 {
		t.Errorf("expected 2 backends, got %d: %+v", len(out.Body), out.Body)
	}

	// Check that the backend URLs contain the expected schemes and paths
	found := make(map[string]bool)
	for _, backend := range out.Body {
		found[backend] = true
	}

	// The actual URLs will be file:// URLs with the configured paths
	expectedCount := 0
	for backend := range found {
		if backend == "file://media/" || backend == "file://backup/" {
			expectedCount++
		}
	}

	if expectedCount != 2 {
		t.Errorf("expected to find both 'file://media/' and 'file://backup/' backends, got: %+v", out.Body)
	}
}
