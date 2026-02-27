package httphandler_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

///////////////////////////////////////////////////////////////////////////////
// RESPONSE HEADER TESTS

func Test_objectGet_contentDispositionHeader(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)
	if err := os.WriteFile(mediaPath+"/hello.txt", []byte("hi"), 0644); err != nil {
		t.Fatal(err)
	}
	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/media/hello.txt", nil)
			rw := httptest.NewRecorder()
			mux.ServeHTTP(rw, req)

			resp := rw.Result()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			cd := resp.Header.Get("Content-Disposition")
			if cd == "" {
				t.Fatal("expected Content-Disposition header to be set")
			}
			if !strings.Contains(cd, "inline") {
				t.Errorf("Content-Disposition should contain 'inline', got %q", cd)
			}
			if !strings.Contains(cd, "hello.txt") {
				t.Errorf("Content-Disposition should contain filename 'hello.txt', got %q", cd)
			}
		})
	}
}

func Test_objectGet_xPathHeader(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath+"/sub")
	if err := os.WriteFile(mediaPath+"/sub/doc.txt", []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/media/sub/doc.txt", nil)
			rw := httptest.NewRecorder()
			mux.ServeHTTP(rw, req)

			resp := rw.Result()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200, got %d", resp.StatusCode)
			}
			if xp := resp.Header.Get("X-Path"); xp != "/sub/doc.txt" {
				t.Errorf("expected X-Path '/sub/doc.txt', got %q", xp)
			}
		})
	}
}

///////////////////////////////////////////////////////////////////////////////
// If-Modified-Since

func Test_objectGet_ifModifiedSince_notModified(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)
	if err := os.WriteFile(mediaPath+"/file.txt", []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// Future timestamp: resource has not been modified since this time.
	future := time.Now().Add(24 * time.Hour)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/media/file.txt", nil)
			req.Header.Set("If-Modified-Since", future.UTC().Format(http.TimeFormat))
			rw := httptest.NewRecorder()
			mux.ServeHTTP(rw, req)

			if rw.Result().StatusCode != http.StatusNotModified {
				t.Errorf("expected 304, got %d", rw.Result().StatusCode)
			}
			if rw.Body.Len() != 0 {
				t.Errorf("expected empty body for 304, got %d bytes", rw.Body.Len())
			}
		})
	}
}

func Test_objectGet_ifModifiedSince_modified(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)
	if err := os.WriteFile(mediaPath+"/file.txt", []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// Past timestamp: resource has been modified since this time.
	past := time.Now().Add(-24 * time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/media/file.txt", nil)
	req.Header.Set("If-Modified-Since", past.UTC().Format(http.TimeFormat))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Result().StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", rw.Result().StatusCode)
	}
	if rw.Body.String() != "content" {
		t.Errorf("expected full body, got %q", rw.Body.String())
	}
}

///////////////////////////////////////////////////////////////////////////////
// If-Unmodified-Since

func Test_objectGet_ifUnmodifiedSince_preconditionFailed(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)
	if err := os.WriteFile(mediaPath+"/file.txt", []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// Past timestamp: resource was modified more recently than this.
	past := time.Now().Add(-24 * time.Hour)

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/media/file.txt", nil)
			req.Header.Set("If-Unmodified-Since", past.UTC().Format(http.TimeFormat))
			rw := httptest.NewRecorder()
			mux.ServeHTTP(rw, req)

			if rw.Result().StatusCode != http.StatusPreconditionFailed {
				t.Errorf("expected 412, got %d", rw.Result().StatusCode)
			}
		})
	}
}

func Test_objectGet_ifUnmodifiedSince_ok(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)
	if err := os.WriteFile(mediaPath+"/file.txt", []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// Future timestamp: resource has not been modified since this time.
	future := time.Now().Add(24 * time.Hour)

	req := httptest.NewRequest(http.MethodGet, "/media/file.txt", nil)
	req.Header.Set("If-Unmodified-Since", future.UTC().Format(http.TimeFormat))
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Result().StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", rw.Result().StatusCode)
	}
}

///////////////////////////////////////////////////////////////////////////////
// If-Match / If-None-Match
// The filesystem backend does not populate ETags. If-Match with any specific
// ETag yields 412 (no match); If-None-Match with a specific ETag yields 200
// (no match, so the condition is not triggered).

func Test_objectGet_ifMatch_noETag_preconditionFailed(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)
	if err := os.WriteFile(mediaPath+"/file.txt", []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := httptest.NewRequest(http.MethodGet, "/media/file.txt", nil)
	req.Header.Set("If-Match", `"abc123"`)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	// No ETag from backend → no match → 412.
	if rw.Result().StatusCode != http.StatusPreconditionFailed {
		t.Errorf("expected 412 when If-Match cannot be satisfied, got %d", rw.Result().StatusCode)
	}
}

func Test_objectGet_ifNoneMatch_noETag_passes(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)
	if err := os.WriteFile(mediaPath+"/file.txt", []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// ETag unknown (empty) → cannot match → 304 shortcut not taken → 200.
	req := httptest.NewRequest(http.MethodGet, "/media/file.txt", nil)
	req.Header.Set("If-None-Match", `"abc123"`)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Result().StatusCode != http.StatusOK {
		t.Errorf("expected 200 when If-None-Match ETag is absent, got %d", rw.Result().StatusCode)
	}
}
