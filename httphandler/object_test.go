package httphandler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

func Test_objectList(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	// Create the directory
	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test listing empty directory
	req := httptest.NewRequest(http.MethodGet, "/api/filer/file/media", nil)
	req.SetPathValue("scheme", "file")
	req.SetPathValue("host", "media")
	req.SetPathValue("path", "/")
	rw := httptest.NewRecorder()

	err = objectList(rw, req, manager, "/api/filer")
	if err != nil {
		t.Fatalf("objectList returned error: %v", err)
	}

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var out schema.ListObjectsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Empty directory should return empty object list
	if len(out.Body) != 0 {
		t.Errorf("expected empty object list, got %+v", out.Body)
	}
}

func Test_objectList_withFiles(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	// Create the directory and some test files
	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create test files
	testFiles := []string{"file1.txt", "file2.txt"}
	for _, filename := range testFiles {
		filepath := mediaPath + "/" + filename
		if err := os.WriteFile(filepath, []byte("test content"), 0644); err != nil {
			t.Fatalf("failed to create test file %s: %v", filename, err)
		}
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test listing directory with files
	req := httptest.NewRequest(http.MethodGet, "/api/filer/file/media", nil)
	req.SetPathValue("scheme", "file")
	req.SetPathValue("host", "media")
	req.SetPathValue("path", "/")
	rw := httptest.NewRecorder()

	err = objectList(rw, req, manager, "/api/filer")
	if err != nil {
		t.Fatalf("objectList returned error: %v", err)
	}

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var out schema.ListObjectsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Should return the created files
	if len(out.Body) != 2 {
		t.Errorf("expected 2 files, got %d: %+v", len(out.Body), out.Body)
	}

	// Verify file URLs
	found := make(map[string]bool)
	for _, obj := range out.Body {
		found[obj.URL] = true
	}

	for _, filename := range testFiles {
		expectedURL := "file://media/" + filename
		if !found[expectedURL] {
			t.Errorf("expected to find file %s in listing", expectedURL)
		}
	}
}

func Test_objectPut(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	// Create the directory
	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test uploading a file
	testContent := "Hello, World!"
	req := httptest.NewRequest(http.MethodPut, "/api/filer/file/media/test.txt", strings.NewReader(testContent))
	req.SetPathValue("scheme", "file")
	req.SetPathValue("host", "media")
	req.SetPathValue("path", "/test.txt")
	req.ContentLength = int64(len(testContent))
	rw := httptest.NewRecorder()

	err = objectPut(rw, req, manager, "/api/filer")
	if err != nil {
		t.Fatalf("objectPut returned error: %v", err)
	}

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201, got %d", resp.StatusCode)
	}

	var out schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify the object was created
	if out.URL != "file://media/test.txt" {
		t.Errorf("expected URL 'file://media/test.txt', got '%s'", out.URL)
	}

	if out.Size != int64(len(testContent)) {
		t.Errorf("expected size %d, got %d", len(testContent), out.Size)
	}
}

func Test_objectPut_existingDirectory(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	// Create the directory and a subdirectory
	if err := os.MkdirAll(mediaPath+"/subdir", 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test uploading a file to a path that exists as a directory
	testContent := "Hello, World!"
	req := httptest.NewRequest(http.MethodPut, "/api/filer/file/media/subdir", strings.NewReader(testContent))
	req.SetPathValue("scheme", "file")
	req.SetPathValue("host", "media")
	req.SetPathValue("path", "/subdir")
	req.ContentLength = int64(len(testContent))
	rw := httptest.NewRecorder()

	err = objectPut(rw, req, manager, "/api/filer")

	// The handler should not return an error, but the HTTP response should be 500
	if err != nil {
		t.Fatalf("objectPut returned unexpected handler error: %v", err)
	}

	// Should return 400 Bad Request when trying to overwrite a directory
	resp := rw.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.StatusCode)
	}

	// Verify the error message indicates it's about overwriting a directory
	body := rw.Body.String()
	if !strings.Contains(body, "cannot overwrite directory") {
		t.Errorf("expected error message to indicate directory overwrite conflict, got: %s", body)
	}
}

func Test_objectList_nonExistentBackend(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test listing from a backend that doesn't exist
	req := httptest.NewRequest(http.MethodGet, "/api/filer/file/nonexistent", nil)
	req.SetPathValue("scheme", "file")
	req.SetPathValue("host", "nonexistent")
	req.SetPathValue("path", "/")
	rw := httptest.NewRecorder()

	err = objectList(rw, req, manager, "/api/filer")
	if err != nil {
		t.Logf("Got expected error for non-existent backend: %v", err)
		return
	}

	resp := rw.Result()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Errorf("expected error for non-existent backend, got status %d", resp.StatusCode)
	}
}

func Test_objectList_nonExistentPath(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test listing a path that doesn't exist
	req := httptest.NewRequest(http.MethodGet, "/api/filer/file/media/nonexistent", nil)
	req.SetPathValue("scheme", "file")
	req.SetPathValue("host", "media")
	req.SetPathValue("path", "/nonexistent")
	rw := httptest.NewRecorder()

	err = objectList(rw, req, manager, "/api/filer")
	if err != nil {
		t.Logf("Got expected error for non-existent path: %v", err)
		return
	}

	resp := rw.Result()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Errorf("expected error for non-existent path, got status %d", resp.StatusCode)
	}
}

func Test_objectList_listFile(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create a test file
	testFile := mediaPath + "/test.txt"
	if err := os.WriteFile(testFile, []byte("test content"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test listing a file (not directory)
	req := httptest.NewRequest(http.MethodGet, "/api/filer/file/media/test.txt", nil)
	req.SetPathValue("scheme", "file")
	req.SetPathValue("host", "media")
	req.SetPathValue("path", "/test.txt")
	rw := httptest.NewRecorder()

	err = objectList(rw, req, manager, "/api/filer")
	if err != nil {
		t.Fatalf("objectList returned error when listing a file: %v", err)
	}

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 when listing a file, got %d", resp.StatusCode)
	}

	var out schema.ListObjectsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// When listing a file (not directory), it should return the file itself
	if len(out.Body) != 1 {
		t.Errorf("expected 1 object when listing a file, got %d: %+v", len(out.Body), out.Body)
	}

	if len(out.Body) > 0 {
		if out.Body[0].URL != "file://media/test.txt" {
			t.Errorf("expected file URL 'file://media/test.txt', got '%s'", out.Body[0].URL)
		}
		if out.Body[0].Size != 12 { // "test content" is 12 bytes
			t.Errorf("expected file size 12, got %d", out.Body[0].Size)
		}
	}
}

func Test_objectPut_nonExistentBackend(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test uploading to a backend that doesn't exist
	testContent := "Hello, World!"
	req := httptest.NewRequest(http.MethodPut, "/api/filer/file/nonexistent/test.txt", strings.NewReader(testContent))
	req.SetPathValue("scheme", "file")
	req.SetPathValue("host", "nonexistent")
	req.SetPathValue("path", "/test.txt")
	req.ContentLength = int64(len(testContent))
	rw := httptest.NewRecorder()

	err = objectPut(rw, req, manager, "/api/filer")
	if err != nil {
		t.Logf("Got expected error for non-existent backend: %v", err)
		return
	}

	resp := rw.Result()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Errorf("expected error for non-existent backend, got status %d", resp.StatusCode)
	}
}

func Test_objectPut_emptyBody(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test uploading an empty file
	req := httptest.NewRequest(http.MethodPut, "/api/filer/file/media/empty.txt", strings.NewReader(""))
	req.SetPathValue("scheme", "file")
	req.SetPathValue("host", "media")
	req.SetPathValue("path", "/empty.txt")
	req.ContentLength = 0
	rw := httptest.NewRecorder()

	err = objectPut(rw, req, manager, "/api/filer")
	if err != nil {
		t.Fatalf("objectPut returned error for empty file: %v", err)
	}

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201 for empty file, got %d", resp.StatusCode)
	}

	var out schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Empty file should have size 0
	if out.Size != 0 {
		t.Errorf("expected size 0 for empty file, got %d", out.Size)
	}
}

func Test_objectPut_overwriteExistingFile(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// First, create a file
	firstContent := "First content"
	req1 := httptest.NewRequest(http.MethodPut, "/api/filer/file/media/test.txt", strings.NewReader(firstContent))
	req1.SetPathValue("scheme", "file")
	req1.SetPathValue("host", "media")
	req1.SetPathValue("path", "/test.txt")
	req1.ContentLength = int64(len(firstContent))
	rw1 := httptest.NewRecorder()

	err = objectPut(rw1, req1, manager, "/api/filer")
	if err != nil {
		t.Fatalf("objectPut returned error for first upload: %v", err)
	}

	// Now overwrite it with new content
	secondContent := "Second content - longer!"
	req2 := httptest.NewRequest(http.MethodPut, "/api/filer/file/media/test.txt", strings.NewReader(secondContent))
	req2.SetPathValue("scheme", "file")
	req2.SetPathValue("host", "media")
	req2.SetPathValue("path", "/test.txt")
	req2.ContentLength = int64(len(secondContent))
	rw2 := httptest.NewRecorder()

	err = objectPut(rw2, req2, manager, "/api/filer")
	if err != nil {
		t.Fatalf("objectPut returned error for second upload: %v", err)
	}

	resp := rw2.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("expected status 201 for file overwrite, got %d", resp.StatusCode)
	}

	var out schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Should have the new content's size
	if out.Size != int64(len(secondContent)) {
		t.Errorf("expected size %d for overwritten file, got %d", len(secondContent), out.Size)
	}
}

func Test_objectGet(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create a test file
	testContent := "Hello, World! This is test content."
	testFile := mediaPath + "/test.txt"
	if err := os.WriteFile(testFile, []byte(testContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test getting a file
	req := httptest.NewRequest(http.MethodGet, "/api/filer/file/media/test.txt", nil)
	req.SetPathValue("scheme", "file")
	req.SetPathValue("host", "media")
	req.SetPathValue("path", "/test.txt")
	rw := httptest.NewRecorder()

	err = objectGet(rw, req, manager, "/api/filer")
	if err != nil {
		t.Fatalf("objectGet returned error: %v", err)
	}

	resp := rw.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// Check Content-Type header
	contentType := resp.Header.Get("Content-Type")
	if contentType != "text/plain; charset=utf-8" {
		t.Errorf("expected Content-Type 'text/plain; charset=utf-8', got '%s'", contentType)
	}

	// Check Content-Length header
	contentLength := resp.Header.Get("Content-Length")
	if contentLength != fmt.Sprint(len(testContent)) {
		t.Errorf("expected Content-Length '%d', got '%s'", len(testContent), contentLength)
	}

	// Check Last-Modified header
	lastModified := resp.Header.Get("Last-Modified")
	if lastModified == "" {
		t.Error("expected Last-Modified header to be set")
	}

	// Check X-Object-Meta header
	objectMeta := resp.Header.Get(schema.ObjectMetaHeader)
	if objectMeta == "" {
		t.Error("expected X-Object-Meta header to be set")
	} else {
		var obj schema.Object
		if err := json.Unmarshal([]byte(objectMeta), &obj); err != nil {
			t.Errorf("failed to decode X-Object-Meta header: %v", err)
		} else {
			if obj.URL != "file://media/test.txt" {
				t.Errorf("expected URL 'file://media/test.txt' in metadata, got '%s'", obj.URL)
			}
			if obj.Size != int64(len(testContent)) {
				t.Errorf("expected size %d in metadata, got %d", len(testContent), obj.Size)
			}
		}
	}

	// Check body content
	body := rw.Body.String()
	if body != testContent {
		t.Errorf("expected body '%s', got '%s'", testContent, body)
	}
}

func Test_objectGet_nonExistentFile(t *testing.T) {
	// Create temporary directory for file backend
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"

	if err := os.MkdirAll(mediaPath, 0755); err != nil {
		t.Fatalf("failed to create media directory: %v", err)
	}

	// Create a manager with a file backend
	manager, err := filer.New(
		context.Background(),
		filer.WithFileBackend("media", mediaPath),
	)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Test getting a file that doesn't exist
	req := httptest.NewRequest(http.MethodGet, "/api/filer/file/media/nonexistent.txt", nil)
	req.SetPathValue("scheme", "file")
	req.SetPathValue("host", "media")
	req.SetPathValue("path", "/nonexistent.txt")
	rw := httptest.NewRecorder()

	err = objectGet(rw, req, manager, "/api/filer")
	if err != nil {
		t.Logf("Got expected error for non-existent file: %v", err)
		return
	}

	resp := rw.Result()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Errorf("expected error for non-existent file, got status %d", resp.StatusCode)
	}
}
