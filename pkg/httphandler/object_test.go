package httphandler_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

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

func Test_objectHead_nonExistentFile(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodHead, "/media/missing.txt", nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Result().StatusCode >= 200 && rw.Result().StatusCode < 300 {
		t.Errorf("expected error for non-existent file, got status %d", rw.Result().StatusCode)
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

func Test_objectDelete_single(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// Create the file
	req := httptest.NewRequest(http.MethodPut, "/media/del.txt", strings.NewReader("bye"))
	req.ContentLength = 3
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Result().StatusCode != http.StatusCreated {
		t.Fatalf("setup PUT failed: %d", rw.Result().StatusCode)
	}

	// Delete it
	req = httptest.NewRequest(http.MethodDelete, "/media/del.txt", nil)
	rw = httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Result().StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rw.Result().StatusCode, rw.Body.String())
	}

	var out schema.Object
	if err := json.NewDecoder(rw.Result().Body).Decode(&out); err != nil {
		t.Fatalf("decode delete response: %v", err)
	}
	if out.Path != "/del.txt" {
		t.Errorf("expected path '/del.txt', got %q", out.Path)
	}

	// Verify it is gone
	req = httptest.NewRequest(http.MethodGet, "/media/del.txt", nil)
	rw = httptest.NewRecorder()
	mux.ServeHTTP(rw, req)
	if rw.Result().StatusCode >= 200 && rw.Result().StatusCode < 300 {
		t.Errorf("expected error after delete, got %d", rw.Result().StatusCode)
	}
}

func Test_objectUpload_rollbackOnFailure(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath+"/subdir")

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// Upload two files: a.txt succeeds, then "subdir" fails because a directory
	// already exists at that path. The rollback must remove a.txt.
	req := newMultipartRequest(t, "/media",
		[][2]string{{"a.txt", "content"}, {"subdir", "data"}},
		nil,
	)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Result().StatusCode >= 200 && rw.Result().StatusCode < 300 {
		t.Errorf("expected error when second file fails, got %d", rw.Result().StatusCode)
	}

	// a.txt must have been rolled back
	if _, err := os.Stat(mediaPath + "/a.txt"); !os.IsNotExist(err) {
		t.Error("expected a.txt to be rolled back after failed upload, but it exists")
	}
}

// newMultipartRequest builds a POST request with one or more "file" form fields.
// Each entry in files is (filename, content).
func newMultipartRequest(t *testing.T, url string, files [][2]string, extraHeaders map[string]string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for _, f := range files {
		fw, err := mw.CreateFormFile("file", f[0])
		if err != nil {
			t.Fatalf("CreateFormFile: %v", err)
		}
		if _, err := fw.Write([]byte(f[1])); err != nil {
			t.Fatalf("write form file: %v", err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, url, &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	return req
}

// newMultipartRequestWithXPath builds a POST request where each file part
// carries a custom X-Path header alongside the filename. Each entry in files
// is (xpath, content); the base name is used for the Content-Disposition
// filename, mirroring what go-client does.
func newMultipartRequestWithXPath(t *testing.T, url string, files [][2]string, extraHeaders map[string]string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for _, f := range files {
		h := textproto.MIMEHeader{}
		// Use the base name in Content-Disposition (stdlib strips dirs anyway);
		// the full relative path is preserved in X-Path.
		base := filepath.Base(f[0])
		h.Set("Content-Disposition", `form-data; name="file"; filename="`+base+`"`)
		h.Set("Content-Type", "application/octet-stream")
		h.Set("X-Path", f[0])
		part, err := mw.CreatePart(h)
		if err != nil {
			t.Fatalf("CreatePart: %v", err)
		}
		if _, err := part.Write([]byte(f[1])); err != nil {
			t.Fatalf("write part: %v", err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, url, &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	return req
}

func Test_objectUpload_xPathPreservesSubdir(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	files := [][2]string{
		{"x/y/z.txt", "deep content"},
		{"a/b/c.txt", "other content"},
	}
	req := newMultipartRequestWithXPath(t, "/media", files, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, rw.Body.String())
	}

	var out []schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != len(files) {
		t.Fatalf("expected %d objects, got %d", len(files), len(out))
	}
	paths := make(map[string]bool)
	for _, o := range out {
		paths[o.Path] = true
	}
	for _, f := range files {
		if !paths["/"+f[0]] {
			t.Errorf("expected path '/%s' in response, got %v", f[0], paths)
		}
	}
}

func Test_objectUpload_xPathTraversalRejected(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// Traversal path should be sanitised to a safe filename, not escape root.
	req := newMultipartRequestWithXPath(t, "/media",
		[][2]string{{"../escape.txt", "evil"}},
		nil,
	)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 (sanitised), got %d: %s", resp.StatusCode, rw.Body.String())
	}

	var out []schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 object, got %d", len(out))
	}
	// The object must not have escaped outside the backend root.
	if strings.Contains(out[0].Path, "..") {
		t.Errorf("traversal not sanitised: path = %q", out[0].Path)
	}
}

func Test_objectUpload_toRoot(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := newMultipartRequest(t, "/media", [][2]string{{"hello.txt", "hello world"}}, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, rw.Body.String())
	}

	var out []schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 object, got %d", len(out))
	}
	if out[0].Path != "/hello.txt" {
		t.Errorf("expected path '/hello.txt', got %q", out[0].Path)
	}
	if out[0].Size != int64(len("hello world")) {
		t.Errorf("expected size %d, got %d", len("hello world"), out[0].Size)
	}
}

func Test_objectUpload_toDirectory(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath+"/subdir")

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := newMultipartRequest(t, "/media/subdir/", [][2]string{{"data.txt", "content"}}, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, rw.Body.String())
	}

	var out []schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 object, got %d", len(out))
	}
	if out[0].Path != "/subdir/data.txt" {
		t.Errorf("expected path '/subdir/data.txt', got %q", out[0].Path)
	}
}

func Test_objectUpload_multipleFiles(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	files := [][2]string{
		{"a.txt", "content a"},
		{"b.txt", "content b"},
		{"c.txt", "content c"},
	}
	req := newMultipartRequest(t, "/media", files, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, rw.Body.String())
	}

	var out []schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != len(files) {
		t.Fatalf("expected %d objects, got %d", len(files), len(out))
	}
	paths := make(map[string]bool)
	for _, o := range out {
		paths[o.Path] = true
	}
	for _, f := range files {
		if !paths["/"+f[0]] {
			t.Errorf("expected path '/%s' in response", f[0])
		}
	}
}

func Test_objectUpload_multipleFilesToDirectory(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath+"/uploads")

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	files := [][2]string{
		{"x.txt", "content x"},
		{"y.txt", "content y"},
	}
	req := newMultipartRequest(t, "/media/uploads/", files, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, rw.Body.String())
	}

	var out []schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != len(files) {
		t.Fatalf("expected %d objects, got %d", len(files), len(out))
	}
	paths := make(map[string]bool)
	for _, o := range out {
		paths[o.Path] = true
	}
	for _, f := range files {
		if !paths["/uploads/"+f[0]] {
			t.Errorf("expected path '/uploads/%s' in response", f[0])
		}
	}
}

func Test_objectUpload_noFiles(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := newMultipartRequest(t, "/media", nil, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for empty upload, got %d", resp.StatusCode)
	}
}

func Test_objectUpload_withSharedMeta(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := newMultipartRequest(t, "/media",
		[][2]string{{"tagged.txt", "tagged content"}},
		map[string]string{"X-Meta-Author": "testuser"},
	)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, rw.Body.String())
	}

	var out []schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 object, got %d", len(out))
	}
	if out[0].Meta["author"] != "testuser" {
		t.Errorf("expected meta author 'testuser', got %q", out[0].Meta["author"])
	}
}

///////////////////////////////////////////////////////////////////////////////
// HELPERS FOR FINE-GRAINED PART CONSTRUCTION

// partSpec describes a single multipart file part with optional per-part headers.
type partSpec struct {
	filename string            // Content-Disposition filename
	content  string            // part body
	headers  map[string]string // extra MIME headers on the part (e.g. X-Meta-*, X-Path)
}

// newMultipartRequestFromSpecs builds a POST request where each element of
// specs becomes one "file" part with fully customisable headers.
func newMultipartRequestFromSpecs(t *testing.T, url string, specs []partSpec, reqHeaders map[string]string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for _, s := range specs {
		h := textproto.MIMEHeader{}
		h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, s.filename))
		h.Set("Content-Type", "application/octet-stream")
		for k, v := range s.headers {
			h.Set(k, v)
		}
		part, err := mw.CreatePart(h)
		if err != nil {
			t.Fatalf("CreatePart: %v", err)
		}
		if _, err := part.Write([]byte(s.content)); err != nil {
			t.Fatalf("write part: %v", err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, url, &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	for k, v := range reqHeaders {
		req.Header.Set(k, v)
	}
	return req
}

///////////////////////////////////////////////////////////////////////////////
// GAP 1: NON-DIRECTORY EXPLICIT PATH

// POST /media/explicit.txt with one file: the file is stored at that exact path
// regardless of the part filename.
func Test_objectUpload_toExplicitPath(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// Part has a different filename to the URL path to confirm the URL wins.
	req := newMultipartRequest(t, "/media/dest.txt", [][2]string{{"original.txt", "explicit content"}}, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, rw.Body.String())
	}

	var out []schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 object, got %d", len(out))
	}
	if out[0].Path != "/dest.txt" {
		t.Errorf("expected path '/dest.txt' (URL wins), got %q", out[0].Path)
	}
	if out[0].Size != int64(len("explicit content")) {
		t.Errorf("expected size %d, got %d", len("explicit content"), out[0].Size)
	}
}

// POST /media/dest.txt with multiple files must be rejected (all would silently
// overwrite the same path).
func Test_objectUpload_multipleFilesToNonDirPath(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := newMultipartRequest(t, "/media/dest.txt", [][2]string{
		{"a.txt", "content a"},
		{"b.txt", "content b"},
	}, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for multiple files to non-dir path, got %d: %s", resp.StatusCode, rw.Body.String())
	}
}

///////////////////////////////////////////////////////////////////////////////
// GAP 2: PER-FILE X-Meta-* HEADERS ON PARTS

// Each part may carry its own X-Meta-* headers; they are attached to the
// resulting object independently.
func Test_objectUpload_perFileMeta(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	specs := []partSpec{
		{filename: "doc.txt", content: "docs", headers: map[string]string{"X-Meta-Category": "docs"}},
		{filename: "img.txt", content: "imgs", headers: map[string]string{"X-Meta-Category": "images"}},
	}
	req := newMultipartRequestFromSpecs(t, "/media", specs, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, rw.Body.String())
	}

	var out []schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(out))
	}

	byPath := make(map[string]schema.Object)
	for _, o := range out {
		byPath[o.Path] = o
	}

	if byPath["/doc.txt"].Meta["category"] != "docs" {
		t.Errorf("/doc.txt: expected meta category 'docs', got %q", byPath["/doc.txt"].Meta["category"])
	}
	if byPath["/img.txt"].Meta["category"] != "images" {
		t.Errorf("/img.txt: expected meta category 'images', got %q", byPath["/img.txt"].Meta["category"])
	}
}

// Per-file X-Meta-* headers override shared request-level X-Meta-* headers for
// the same key, while non-conflicting shared keys still propagate.
func Test_objectUpload_metaMergeSharedAndPerFile(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// Shared header: author=shared, project=myproject
	// Part override for first file: author=override
	specs := []partSpec{
		{filename: "a.txt", content: "a", headers: map[string]string{"X-Meta-Author": "override"}},
		{filename: "b.txt", content: "b"},
	}
	req := newMultipartRequestFromSpecs(t, "/media", specs, map[string]string{
		"X-Meta-Author":  "shared",
		"X-Meta-Project": "myproject",
	})
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, rw.Body.String())
	}

	var out []schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(out))
	}

	byPath := make(map[string]schema.Object)
	for _, o := range out {
		byPath[o.Path] = o
	}

	// a.txt: per-file author overrides shared; project inherited from shared
	if byPath["/a.txt"].Meta["author"] != "override" {
		t.Errorf("/a.txt: expected author 'override', got %q", byPath["/a.txt"].Meta["author"])
	}
	if byPath["/a.txt"].Meta["project"] != "myproject" {
		t.Errorf("/a.txt: expected project 'myproject', got %q", byPath["/a.txt"].Meta["project"])
	}
	// b.txt: no per-file override; shared author and project both flow through
	if byPath["/b.txt"].Meta["author"] != "shared" {
		t.Errorf("/b.txt: expected author 'shared', got %q", byPath["/b.txt"].Meta["author"])
	}
	if byPath["/b.txt"].Meta["project"] != "myproject" {
		t.Errorf("/b.txt: expected project 'myproject', got %q", byPath["/b.txt"].Meta["project"])
	}
}

///////////////////////////////////////////////////////////////////////////////
// GAP 3: CONTENT-TYPE SNIFFING FROM EXTENSION

// When a part carries application/octet-stream (the multipart default), the
// handler sniffs the MIME type from the filename extension and stores it.
func Test_objectUpload_contentTypeFromExtension(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// newMultipartRequest sets Content-Type: application/octet-stream on each part.
	req := newMultipartRequest(t, "/media", [][2]string{{"readme.txt", "plain text content"}}, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	resp := rw.Result()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, rw.Body.String())
	}

	var out []schema.Object
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 object, got %d", len(out))
	}
	// The handler must have sniffed text/plain from .txt; the exact charset
	// parameter may vary by platform so check the base type.
	if !strings.HasPrefix(out[0].ContentType, "text/plain") {
		t.Errorf("expected ContentType starting with 'text/plain', got %q", out[0].ContentType)
	}
}

///////////////////////////////////////////////////////////////////////////////
// GAP 4: METHOD NOT ALLOWED

// Unsupported methods on both upload routes must return 405.
func Test_objectUpload_methodNotAllowed(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	for _, tc := range []struct {
		method string
		url    string
	}{
		{http.MethodPatch, "/media"},
		{http.MethodPatch, "/media/somefile.txt"},
		{http.MethodOptions, "/media"},
		{http.MethodOptions, "/media/somefile.txt"},
	} {
		t.Run(tc.method+" "+tc.url, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.url, nil)
			rw := httptest.NewRecorder()
			mux.ServeHTTP(rw, req)
			if rw.Result().StatusCode != http.StatusMethodNotAllowed {
				t.Errorf("expected 405, got %d", rw.Result().StatusCode)
			}
		})
	}
}
