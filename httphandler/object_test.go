package httphandler_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
)

func Test_objectList(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

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

func Test_objectPut(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	testContent := "Hello, World!"
	req := httptest.NewRequest(http.MethodPut, "/media/test.txt", strings.NewReader(testContent))
	req.ContentLength = int64(len(testContent))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201, got %d", resp.StatusCode)
	}

	var out schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if out.Name != "media" || out.Path != "/test.txt" {
		t.Errorf("expected name 'media' and path '/test.txt', got name '%s' path '%s'", out.Name, out.Path)
	}
	if out.Size != int64(len(testContent)) {
		t.Errorf("expected size %d, got %d", len(testContent), out.Size)
	}
}

func Test_objectPut_withHeaders(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	testContent := "plain text content"
	req := httptest.NewRequest(http.MethodPut, "/media/meta.txt", strings.NewReader(testContent))
	req.ContentLength = int64(len(testContent))
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("X-Meta-Author", "testuser")
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", resp.StatusCode)
	}

	var out schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if out.ContentType != "text/plain; charset=utf-8" {
		t.Errorf("expected ContentType 'text/plain; charset=utf-8', got %q", out.ContentType)
	}
	if out.Meta["author"] != "testuser" {
		t.Errorf("expected meta author 'testuser', got %q", out.Meta["author"])
	}
}

func Test_objectPut_existingDirectory(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath+"/subdir")

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	testContent := "Hello, World!"
	req := httptest.NewRequest(http.MethodPut, "/media/subdir", strings.NewReader(testContent))
	req.ContentLength = int64(len(testContent))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}

	body := rw.Body.String()
	if !strings.Contains(body, "cannot overwrite directory") {
		t.Errorf("expected error message about directory overwrite, got: %s", body)
	}
}

func Test_objectPut_nonExistentBackend(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	testContent := "Hello, World!"
	req := httptest.NewRequest(http.MethodPut, "/nonexistent/test.txt", strings.NewReader(testContent))
	req.ContentLength = int64(len(testContent))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Errorf("expected error for non-existent backend, got status %d", resp.StatusCode)
	}
}

func Test_objectPut_emptyBody(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodPut, "/media/empty.txt", strings.NewReader(""))
	req.ContentLength = 0
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201 for empty file, got %d", resp.StatusCode)
	}

	var out schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if out.Size != 0 {
		t.Errorf("expected size 0 for empty file, got %d", out.Size)
	}
}

func Test_objectPut_overwriteExistingFile(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	firstContent := "First content"
	req1 := httptest.NewRequest(http.MethodPut, "/media/test.txt", strings.NewReader(firstContent))
	req1.ContentLength = int64(len(firstContent))
	rw1 := httptest.NewRecorder()
	mux.ServeHTTP(rw1, req1)
	if rw1.Result().StatusCode != http.StatusCreated {
		t.Fatalf("first PUT failed with status %d", rw1.Result().StatusCode)
	}

	secondContent := "Second content - longer!"
	req2 := httptest.NewRequest(http.MethodPut, "/media/test.txt", strings.NewReader(secondContent))
	req2.ContentLength = int64(len(secondContent))
	rw2 := httptest.NewRecorder()
	mux.ServeHTTP(rw2, req2)

	resp := rw2.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201 for file overwrite, got %d", resp.StatusCode)
	}

	var out schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if out.Size != int64(len(secondContent)) {
		t.Errorf("expected size %d for overwritten file, got %d", len(secondContent), out.Size)
	}
}

func Test_objectGet(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	testContent := "Hello, World! This is test content."
	if err := os.WriteFile(mediaPath+"/test.txt", []byte(testContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodGet, "/media/test.txt", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "text/plain; charset=utf-8" {
		t.Errorf("expected Content-Type 'text/plain; charset=utf-8', got '%s'", contentType)
	}

	contentLength := resp.Header.Get("Content-Length")
	if contentLength != fmt.Sprint(len(testContent)) {
		t.Errorf("expected Content-Length '%d', got '%s'", len(testContent), contentLength)
	}

	if resp.Header.Get("Last-Modified") == "" {
		t.Error("expected Last-Modified header to be set")
	}

	objectMeta := resp.Header.Get(schema.ObjectMetaHeader)
	if objectMeta == "" {
		t.Error("expected X-Object-Meta header to be set")
	} else {
		var obj schema.Object
		if err := json.Unmarshal([]byte(objectMeta), &obj); err != nil {
			t.Errorf("failed to decode X-Object-Meta header: %v", err)
		} else {
			if obj.Name != "media" || obj.Path != "/test.txt" {
				t.Errorf("expected name 'media' and path '/test.txt' in metadata, got name '%s' path '%s'", obj.Name, obj.Path)
			}
			if obj.Size != int64(len(testContent)) {
				t.Errorf("expected size %d in metadata, got %d", len(testContent), obj.Size)
			}
		}
	}

	body := rw.Body.String()
	if body != testContent {
		t.Errorf("expected body '%s', got '%s'", testContent, body)
	}
}

func Test_objectGet_nonExistentFile(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodGet, "/media/nonexistent.txt", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Errorf("expected error for non-existent file, got status %d", resp.StatusCode)
	}
}

func Test_objectHead(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	testContent := "head test content"
	if err := os.WriteFile(mediaPath+"/head.txt", []byte(testContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodHead, "/media/head.txt", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
	if rw.Body.Len() != 0 {
		t.Errorf("expected empty body for HEAD, got %d bytes", rw.Body.Len())
	}
	if resp.Header.Get("Content-Length") != fmt.Sprint(len(testContent)) {
		t.Errorf("expected Content-Length %d, got %s", len(testContent), resp.Header.Get("Content-Length"))
	}
	if resp.Header.Get("Last-Modified") == "" {
		t.Error("expected Last-Modified header to be set")
	}
	metaHeader := resp.Header.Get(schema.ObjectMetaHeader)
	if metaHeader == "" {
		t.Error("expected X-Object-Meta header to be set")
	} else {
		var obj schema.Object
		if err := json.Unmarshal([]byte(metaHeader), &obj); err != nil {
			t.Errorf("failed to decode X-Object-Meta header: %v", err)
		} else if obj.Name != "media" || obj.Path != "/head.txt" {
			t.Errorf("unexpected meta: name=%q path=%q", obj.Name, obj.Path)
		}
	}
}

func Test_objectDelete_recursive(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath+"/subdir")

	files := []string{"subdir/a.txt", "subdir/b.txt"}
	for _, f := range files {
		if err := os.WriteFile(mediaPath+"/"+f, []byte("content"), 0644); err != nil {
			t.Fatalf("failed to create %s: %v", f, err)
		}
	}

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodDelete, "/media/subdir?recursive=true", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var out schema.DeleteObjectsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(out.Body) != 2 {
		t.Errorf("expected 2 deleted objects, got %d: %+v", len(out.Body), out.Body)
	}

	// Verify files are gone
	for _, f := range files {
		if _, err := os.Stat(mediaPath + "/" + f); !os.IsNotExist(err) {
			t.Errorf("expected %s to be deleted", f)
		}
	}
}
