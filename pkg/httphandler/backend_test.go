package httphandler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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

	// Body is a map of name → URL string; check keys exist, values are correct, and no credentials are leaked
	for _, tc := range []struct {
		name string
		path string
	}{
		{"media", mediaPath},
		{"backup", backupPath},
	} {
		rawURL, ok := out.Body[tc.name]
		if !ok {
			t.Errorf("expected %q key in backends, got: %+v", tc.name, out.Body)
			continue
		}
		u, err := url.Parse(rawURL)
		if err != nil {
			t.Errorf("backend %q URL %q is not parseable: %v", tc.name, rawURL, err)
			continue
		}
		if u.Scheme != "file" {
			t.Errorf("backend %q: expected scheme %q, got %q", tc.name, "file", u.Scheme)
		}
		if u.Host != tc.name {
			t.Errorf("backend %q: expected host %q, got %q", tc.name, tc.name, u.Host)
		}
		if u.Path != tc.path {
			t.Errorf("backend %q: expected path %q, got %q", tc.name, tc.path, u.Path)
		}
		if u.User != nil {
			t.Errorf("backend %q: URL must not contain userinfo, got %q", tc.name, rawURL)
		}
		if u.RawQuery != "" {
			t.Errorf("backend %q: file:// URL must have no query params, got %q", tc.name, u.RawQuery)
		}
	}
}
