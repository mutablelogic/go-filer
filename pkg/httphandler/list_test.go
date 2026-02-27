package httphandler_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

func Test_objectList(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodGet, "/media?limit=100", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var out schema.ListObjectsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(out.Body) != 0 {
		t.Errorf("expected empty object list, got %+v", out.Body)
	}
}

func Test_objectList_withFiles(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	testFiles := []string{"file1.txt", "file2.txt"}
	for _, filename := range testFiles {
		fp := mediaPath + "/" + filename
		if err := os.WriteFile(fp, []byte("test content"), 0644); err != nil {
			t.Fatalf("failed to create test file %s: %v", filename, err)
		}
	}

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodGet, "/media?limit=100", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var out schema.ListObjectsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(out.Body) != 2 {
		t.Errorf("expected 2 files, got %d: %+v", len(out.Body), out.Body)
	}

	found := make(map[string]bool)
	for _, obj := range out.Body {
		found[obj.Name+obj.Path] = true
	}
	for _, filename := range testFiles {
		expectedKey := "media/" + filename
		if !found[expectedKey] {
			t.Errorf("expected to find file %s in listing", expectedKey)
		}
	}
}

func Test_objectList_countOnly(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	for _, filename := range []string{"a.txt", "b.txt", "c.txt"} {
		fp := mediaPath + "/" + filename
		if err := os.WriteFile(fp, []byte("x"), 0644); err != nil {
			t.Fatalf("failed to create test file %s: %v", filename, err)
		}
	}

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// limit=0 (omitted) returns count only, no body
	req := httptest.NewRequest(http.MethodGet, "/media", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var out schema.ListObjectsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if out.Count != 3 {
		t.Errorf("expected count=3, got %d", out.Count)
	}
	if len(out.Body) != 0 {
		t.Errorf("expected no body for limit=0, got %+v", out.Body)
	}
}

func Test_objectList_nonExistentBackend(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Errorf("expected error for non-existent backend, got status %d", resp.StatusCode)
	}
}

// Test_objectList_recursive verifies that without ?recursive=true only
// immediate children are returned, and that adding it exposes the full tree.
func Test_objectList_recursive(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath+"/subdir")

	// root file + one file inside subdir
	for _, f := range []string{"root.txt", "subdir/child.txt"} {
		if err := os.WriteFile(mediaPath+"/"+f, []byte("x"), 0644); err != nil {
			t.Fatalf("create %s: %v", f, err)
		}
	}

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	t.Run("NonRecursive", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/media", nil)
		rw := httptest.NewRecorder()
		mux.ServeHTTP(rw, req)

		resp := rw.Result()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		var out schema.ListObjectsResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		// Non-recursive: root.txt and subdir entry; child.txt must not appear.
		paths := make(map[string]bool)
		for _, o := range out.Body {
			paths[o.Path] = true
		}
		if paths["/subdir/child.txt"] {
			t.Errorf("non-recursive listing must not include /subdir/child.txt")
		}
	})

	t.Run("Recursive", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/media?recursive=true&limit=100", nil)
		rw := httptest.NewRecorder()
		mux.ServeHTTP(rw, req)

		resp := rw.Result()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
		var out schema.ListObjectsResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		// Recursive: both root.txt and subdir/child.txt must appear.
		paths := make(map[string]bool)
		for _, o := range out.Body {
			paths[o.Path] = true
		}
		if !paths["/root.txt"] {
			t.Errorf("recursive listing missing /root.txt; got %v", paths)
		}
		if !paths["/subdir/child.txt"] {
			t.Errorf("recursive listing missing /subdir/child.txt; got %v", paths)
		}
	})
}

// Test_objectList_pathFilter verifies that ?path=/subdir restricts the listing
// to objects whose path starts with the given prefix.
func Test_objectList_pathFilter(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath+"/subdir")
	mustMkDir(t, mediaPath+"/other")

	for _, f := range []string{"root.txt", "subdir/a.txt", "subdir/b.txt", "other/c.txt"} {
		if err := os.WriteFile(mediaPath+"/"+f, []byte("x"), 0644); err != nil {
			t.Fatalf("create %s: %v", f, err)
		}
	}

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodGet, "/media?path=/subdir&recursive=true&limit=100", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, rw.Body.String())
	}
	var out schema.ListObjectsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Only subdir/a.txt and subdir/b.txt should appear.
	paths := make(map[string]bool)
	for _, o := range out.Body {
		paths[o.Path] = true
	}
	if !paths["/subdir/a.txt"] {
		t.Errorf("path filter missing /subdir/a.txt; got %v", paths)
	}
	if !paths["/subdir/b.txt"] {
		t.Errorf("path filter missing /subdir/b.txt; got %v", paths)
	}
	if paths["/root.txt"] {
		t.Errorf("path filter must not include /root.txt")
	}
	if paths["/other/c.txt"] {
		t.Errorf("path filter must not include /other/c.txt")
	}
}

// Test_objectList_methodNotAllowed verifies that methods other than GET and POST
// on /{name} return 405.
func Test_objectList_methodNotAllowed(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	for _, method := range []string{
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
		http.MethodOptions,
	} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/media", nil)
			rw := httptest.NewRecorder()
			mux.ServeHTTP(rw, req)
			if rw.Result().StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("expected 405, got %d", rw.Result().StatusCode)
			}
		})
	}
}
