package httphandler_test

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

///////////////////////////////////////////////////////////////////////////////
// HELPERS

// sseEvent holds one parsed Server-Sent Event.
type sseEvent struct {
	Name string
	Data string // raw JSON string, empty for ping events
}

// parseSSEEvents parses a text/event-stream body into a slice of sseEvents.
// Ping events (name == "ping", no data) are included so callers can filter
// them if needed.
func parseSSEEvents(body string) []sseEvent {
	var events []sseEvent
	var name, data string

	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		case line == "":
			if name != "" {
				events = append(events, sseEvent{Name: name, Data: data})
			}
			name, data = "", ""
		}
	}
	// Flush any trailing event without a final blank line.
	if name != "" {
		events = append(events, sseEvent{Name: name, Data: data})
	}
	return events
}

// sseEventsByName filters a slice keeping only events with the given name.
func sseEventsByName(events []sseEvent, name string) []sseEvent {
	var out []sseEvent
	for _, e := range events {
		if e.Name == name {
			out = append(out, e)
		}
	}
	return out
}

// newSSEUploadRequest wraps newMultipartRequest and adds Accept: text/event-stream.
func newSSEUploadRequest(t *testing.T, url string, files [][2]string, extraHeaders map[string]string) *http.Request {
	t.Helper()
	req := newMultipartRequest(t, url, files, extraHeaders)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-Upload-Count", strconv.Itoa(len(files)))
	return req
}

///////////////////////////////////////////////////////////////////////////////
// TESTS

// Test_objectUploadSSE_singleFile uploads one file in SSE mode and verifies
// the full event sequence: start → file (written=0) → complete → done.
func Test_objectUploadSSE_singleFile(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	content := "hello world"
	req := newSSEUploadRequest(t, "/media", [][2]string{{"hello.txt", content}}, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	if ct := rw.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("expected Content-Type text/event-stream, got %q", ct)
	}

	events := parseSSEEvents(rw.Body.String())

	// start event
	starts := sseEventsByName(events, schema.UploadStartEvent)
	if len(starts) != 1 {
		t.Fatalf("expected 1 start event, got %d", len(starts))
	}
	var start schema.UploadStart
	if err := json.Unmarshal([]byte(starts[0].Data), &start); err != nil {
		t.Fatalf("unmarshal start: %v", err)
	}
	if start.Files != 1 {
		t.Errorf("start.Files: want 1, got %d", start.Files)
	}

	// file event (written == 0 = file-start signal)
	fileEvts := sseEventsByName(events, schema.UploadFileEvent)
	if len(fileEvts) == 0 {
		t.Fatal("expected at least one file event")
	}
	var fe schema.UploadFile
	if err := json.Unmarshal([]byte(fileEvts[0].Data), &fe); err != nil {
		t.Fatalf("unmarshal file event: %v", err)
	}
	if fe.Index != 0 {
		t.Errorf("file event index: want 0, got %d", fe.Index)
	}
	if fe.Written != 0 {
		t.Errorf("first file event written: want 0, got %d", fe.Written)
	}

	// complete event
	completes := sseEventsByName(events, schema.UploadCompleteEvent)
	if len(completes) != 1 {
		t.Fatalf("expected 1 complete event, got %d", len(completes))
	}
	var obj schema.Object
	if err := json.Unmarshal([]byte(completes[0].Data), &obj); err != nil {
		t.Fatalf("unmarshal complete: %v", err)
	}
	if obj.Path != "/hello.txt" {
		t.Errorf("complete path: want /hello.txt, got %q", obj.Path)
	}
	if obj.Size != int64(len(content)) {
		t.Errorf("complete size: want %d, got %d", len(content), obj.Size)
	}

	// done event
	dones := sseEventsByName(events, schema.UploadDoneEvent)
	if len(dones) != 1 {
		t.Fatalf("expected 1 done event, got %d", len(dones))
	}
	var done schema.UploadDone
	if err := json.Unmarshal([]byte(dones[0].Data), &done); err != nil {
		t.Fatalf("unmarshal done: %v", err)
	}
	if done.Files != 1 {
		t.Errorf("done.Files: want 1, got %d", done.Files)
	}
	if done.Bytes != int64(len(content)) {
		t.Errorf("done.Bytes: want %d, got %d", len(content), done.Bytes)
	}
}

// Test_objectUploadSSE_multipleFiles uploads three files in SSE mode and checks
// that each file produces a file-start and complete event and done.Files == 3.
func Test_objectUploadSSE_multipleFiles(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	files := [][2]string{
		{"a.txt", "aaa"},
		{"b.txt", "bbbb"},
		{"c.txt", "ccccc"},
	}
	req := newSSEUploadRequest(t, "/media", files, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}

	events := parseSSEEvents(rw.Body.String())

	// One start event with Files == 3.
	starts := sseEventsByName(events, schema.UploadStartEvent)
	if len(starts) != 1 {
		t.Fatalf("expected 1 start, got %d", len(starts))
	}
	var start schema.UploadStart
	if err := json.Unmarshal([]byte(starts[0].Data), &start); err != nil {
		t.Fatalf("unmarshal start: %v", err)
	}
	if start.Files != 3 {
		t.Errorf("start.Files: want 3, got %d", start.Files)
	}

	// Three file-start events (written == 0), one per file, in order.
	allFileEvts := sseEventsByName(events, schema.UploadFileEvent)
	fileStarts := make([]sseEvent, 0, 3)
	for _, e := range allFileEvts {
		var fe schema.UploadFile
		if err := json.Unmarshal([]byte(e.Data), &fe); err == nil && fe.Written == 0 {
			fileStarts = append(fileStarts, e)
		}
	}
	if len(fileStarts) != 3 {
		t.Errorf("expected 3 file-start events, got %d", len(fileStarts))
	}

	// Three complete events, paths must match uploaded files.
	completes := sseEventsByName(events, schema.UploadCompleteEvent)
	if len(completes) != 3 {
		t.Fatalf("expected 3 complete events, got %d", len(completes))
	}
	paths := make(map[string]bool)
	for _, e := range completes {
		var obj schema.Object
		if err := json.Unmarshal([]byte(e.Data), &obj); err != nil {
			t.Fatalf("unmarshal complete: %v", err)
		}
		paths[obj.Path] = true
	}
	for _, f := range files {
		if !paths["/"+f[0]] {
			t.Errorf("missing complete event for /%s", f[0])
		}
	}

	// One done event with Files == 3 and Bytes == sum of content lengths.
	dones := sseEventsByName(events, schema.UploadDoneEvent)
	if len(dones) != 1 {
		t.Fatalf("expected 1 done, got %d", len(dones))
	}
	var done schema.UploadDone
	if err := json.Unmarshal([]byte(dones[0].Data), &done); err != nil {
		t.Fatalf("unmarshal done: %v", err)
	}
	if done.Files != 3 {
		t.Errorf("done.Files: want 3, got %d", done.Files)
	}
	var wantBytes int64
	for _, f := range files {
		wantBytes += int64(len(f[1]))
	}
	if done.Bytes != wantBytes {
		t.Errorf("done.Bytes: want %d, got %d", wantBytes, done.Bytes)
	}
}

// Test_objectUploadSSE_errorRollback uploads two files where the second fails
// (attempting to overwrite an existing directory). Verifies:
//   - an error event is emitted with the correct index
//   - no done event is emitted
//   - the first file is rolled back (deleted)
func Test_objectUploadSSE_errorRollback(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath+"/subdir")

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	req := newSSEUploadRequest(t, "/media",
		[][2]string{{"a.txt", "content"}, {"subdir", "data"}},
		nil,
	)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	// SSE always responds 200 — error is conveyed in-band.
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}

	events := parseSSEEvents(rw.Body.String())

	// Must have an error event for index 1.
	errs := sseEventsByName(events, schema.UploadErrorEvent)
	if len(errs) != 1 {
		t.Fatalf("expected 1 error event, got %d", len(errs))
	}
	var ue schema.UploadError
	if err := json.Unmarshal([]byte(errs[0].Data), &ue); err != nil {
		t.Fatalf("unmarshal error event: %v", err)
	}
	if ue.Index != 1 {
		t.Errorf("error event index: want 1, got %d", ue.Index)
	}

	// No done event.
	if dones := sseEventsByName(events, schema.UploadDoneEvent); len(dones) != 0 {
		t.Errorf("expected no done event on error, got %d", len(dones))
	}

	// a.txt must have been rolled back.
	if _, err := os.Stat(mediaPath + "/a.txt"); !os.IsNotExist(err) {
		t.Error("a.txt should have been rolled back but still exists")
	}
}

// Test_objectUploadSSE_largeFile uploads a ~128 KiB file in SSE mode to
// exercise the progressReader's mid-transfer emit loop (triggered every 64 KiB).
// We verify that at least one progress file-event with Written > 0 is emitted.
func Test_objectUploadSSE_largeFile(t *testing.T) {
	tempDir := t.TempDir()
	mediaPath := tempDir + "/media"
	mustMkDir(t, mediaPath)

	mgr := newTestManager(t, "file://media"+mediaPath)
	mux := serveMux(mgr)

	// 128 KiB of content → the 64 KiB chunk boundary will be crossed at least once.
	const size = 128 * 1024
	content := make([]byte, size)
	for i := range content {
		content[i] = 'x'
	}

	req := newSSEUploadRequest(t, "/media", [][2]string{{"large.bin", string(content)}}, nil)
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}

	events := parseSSEEvents(rw.Body.String())

	// At least one file event with Written > 0 (the progress tick).
	allFileEvts := sseEventsByName(events, schema.UploadFileEvent)
	hasMidProgress := false
	for _, e := range allFileEvts {
		var fe schema.UploadFile
		if err := json.Unmarshal([]byte(e.Data), &fe); err == nil && fe.Written > 0 {
			hasMidProgress = true
			break
		}
	}
	if !hasMidProgress {
		t.Errorf("expected at least one file event with Written > 0 for a %d-byte upload; file events: %v", size, allFileEvts)
	}

	// Verify upload completed successfully.
	dones := sseEventsByName(events, schema.UploadDoneEvent)
	if len(dones) != 1 {
		t.Fatalf("expected 1 done event, got %d", len(dones))
	}
	var done schema.UploadDone
	if err := json.Unmarshal([]byte(dones[0].Data), &done); err != nil {
		t.Fatalf("unmarshal done: %v", err)
	}
	if done.Bytes != size {
		t.Errorf("done.Bytes: want %d, got %d", size, done.Bytes)
	}
}
