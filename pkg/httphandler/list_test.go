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
		found[obj.Path] = true
	}
	for _, filename := range testFiles {
		expectedKey := "/" + filename
		if !found[expectedKey] {
			t.Errorf("expected to find file %s in listing", expectedKey)
		}
	}
}

// Test_objectList_countOnly verifies that omitting ?limit (i.e. limit=0) returns
// the total count but no body — Limit=0 means count-only.
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

	// limit=0 (omitted) returns count only, no body.
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
		req := httptest.NewRequest(http.MethodGet, "/media?limit=100", nil)
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
		// Non-recursive: root.txt must appear; subdir/child.txt must not.
		paths := make(map[string]bool)
		for _, o := range out.Body {
			paths[o.Path] = true
		}
		if !paths["/root.txt"] {
			t.Errorf("non-recursive listing missing /root.txt; got %v", paths)
		}
		if paths["/subdir/child.txt"] {
			t.Errorf("non-recursive listing must not include /subdir/child.txt; got %v", paths)
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

// Test_objectList_isDirNonRecursive verifies that a non-recursive listing returns
// directory placeholder entries with IsDir=true for sub-directories.
func Test_objectList_isDirNonRecursive(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath+"/subdir")

	// A file at root and one inside the subdir.
	if err := os.WriteFile(mediaPath+"/root.txt", []byte("x"), 0644); err != nil {
		t.Fatalf("create root.txt: %v", err)
	}
	if err := os.WriteFile(mediaPath+"/subdir/child.txt", []byte("x"), 0644); err != nil {
		t.Fatalf("create child.txt: %v", err)
	}

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// Non-recursive listing of the root — should see root.txt and subdir/ as a directory.
	req := httptest.NewRequest(http.MethodGet, "/media?limit=100", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rw.Result().StatusCode, rw.Body.String())
	}

	var out schema.ListObjectsResponse
	if err := json.NewDecoder(rw.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	var foundDir, foundFile bool
	for _, obj := range out.Body {
		if obj.IsDir {
			foundDir = true
			if obj.Size != 0 {
				t.Errorf("directory entry %q should have Size=0, got %d", obj.Path, obj.Size)
			}
		}
		if obj.Path == "/root.txt" && !obj.IsDir {
			foundFile = true
		}
	}
	if !foundDir {
		t.Errorf("expected at least one IsDir=true entry in non-recursive listing; got %+v", out.Body)
	}
	if !foundFile {
		t.Errorf("expected /root.txt with IsDir=false in listing; got %+v", out.Body)
	}
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

// Test_objectList_methodNotAllowed verifies that methods other than GET, POST,
// and DELETE on /{name} return 405.
func Test_objectList_methodNotAllowed(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	for _, method := range []string{
		http.MethodPut,
		http.MethodPatch,
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

// Test_objectList_deleteRoot exercises the three DELETE /{name} variants:
//
//	DELETE /media                 — no ?recursive, defaults to Recursive=false
//	DELETE /media?recursive=false — explicit non-recursive
//	DELETE /media?recursive=true  — recursive, removes entire tree
//
// Non-recursive runs verify that root-level objects are deleted while nested
// objects survive. The recursive run verifies both are removed.
func Test_objectList_deleteRoot(t *testing.T) {
	setup := func(t *testing.T) (string, string) {
		t.Helper()
		tempDir := t.TempDir()
		mediaPath := tempDir + "/media"
		mustMkDir(t, mediaPath+"/subdir")
		for _, f := range []string{"root.txt", "subdir/nested.txt"} {
			if err := os.WriteFile(mediaPath+"/"+f, []byte("x"), 0644); err != nil {
				t.Fatalf("create %s: %v", f, err)
			}
		}
		return tempDir, mediaPath
	}

	for _, tc := range []struct {
		url        string
		rootGone   bool // expect root.txt deleted
		nestedGone bool // expect subdir/nested.txt deleted
	}{
		{"/media", true, false},                 // default: non-recursive
		{"/media?recursive=false", true, false}, // explicit non-recursive
		{"/media?recursive=true", true, true},   // recursive: wipes everything
	} {
		t.Run(tc.url, func(t *testing.T) {
			_, mediaPath := setup(t)
			mgr := newTestManager(t, "file://media"+mediaPath)
			mux := serveMux(mgr)

			req := httptest.NewRequest(http.MethodDelete, tc.url, nil)
			rw := httptest.NewRecorder()
			mux.ServeHTTP(rw, req)

			if rw.Result().StatusCode != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", rw.Result().StatusCode, rw.Body.String())
			}

			// Response must be a valid JSON array (DeleteObjectsResponse).
			var resp schema.DeleteObjectsResponse
			if err := json.NewDecoder(rw.Result().Body).Decode(&resp); err != nil {
				t.Fatalf("decode response: %v", err)
			}

			rootMissing := func() bool {
				_, err := os.Stat(mediaPath + "/root.txt")
				return os.IsNotExist(err)
			}
			nestedMissing := func() bool {
				_, err := os.Stat(mediaPath + "/subdir/nested.txt")
				return os.IsNotExist(err)
			}

			if tc.rootGone && !rootMissing() {
				t.Error("expected root.txt to be deleted")
			}
			if !tc.rootGone && rootMissing() {
				t.Error("expected root.txt to survive")
			}
			if tc.nestedGone && !nestedMissing() {
				t.Error("expected subdir/nested.txt to be deleted")
			}
			if !tc.nestedGone && nestedMissing() {
				t.Error("expected subdir/nested.txt to survive")
			}
		})
	}
}
