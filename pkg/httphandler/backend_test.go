package httphandler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	// Packages
	manager "github.com/mutablelogic/go-filer/pkg/manager"
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

func Test_backendList(t *testing.T) {
	mgr, err := manager.New(context.Background())
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var out schema.BackendListResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(out.Body) != 0 {
		t.Errorf("expected empty backends map, got %+v", out.Body)
	}
}

func Test_backendList_methodNotAllowed(t *testing.T) {
	mgr, err := manager.New(context.Background())
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.StatusCode)
	}
}

func Test_backendList_withBackends(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	backupPath := tempDir + "/backup"
	mustMkDir(t, mediaPath)
	mustMkDir(t, backupPath)

	mgr := newTestManager(t,
		"file://media"+mediaPath,
		"file://backup"+backupPath,
	)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var out schema.BackendListResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(out.Body) != 2 {
		t.Errorf("expected 2 backends, got %d: %+v", len(out.Body), out.Body)
	}

	// Body is a map of name → URL string
	if _, ok := out.Body["media"]; !ok {
		t.Errorf("expected 'media' key in backends, got: %+v", out.Body)
	}
	if _, ok := out.Body["backup"]; !ok {
		t.Errorf("expected 'backup' key in backends, got: %+v", out.Body)
	}
}
