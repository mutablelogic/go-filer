package httpclient_test

import (
	"context"
	"io/fs"
	"sort"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	// Packages
	httpclient "github.com/mutablelogic/go-filer/pkg/httpclient"
)

func TestCreateObjects_walk(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	// Build an in-memory filesystem with a small tree.
	memFS := fstest.MapFS{
		"a.txt":          {Data: []byte("hello")},
		"sub/b.txt":      {Data: []byte("world")},
		"sub/deep/c.txt": {Data: []byte("deep")},
		"skip.bin":       {Data: []byte("ignore me")},
	}

	// Filter: include only non-.bin entries.
	filter := func(d fs.DirEntry) bool {
		return strings.HasSuffix(d.Name(), ".bin") == false
	}

	// Count progress events.
	var progressCalls int
	progress := func(_ int, _ int, path string, _, _ int64) {
		if path == "" {
			t.Errorf("progress called with empty path")
		}
		progressCalls++
	}

	objs, err := c.CreateObjects(context.Background(), "testbucket", memFS,
		httpclient.WithFilter(filter),
		httpclient.WithProgress(progress),
	)
	if err != nil {
		t.Fatalf("CreateObjects error: %v", err)
	}

	// Expect the 3 non-.bin files to be uploaded.
	if len(objs) != 3 {
		t.Fatalf("expected 3 objects, got %d: %+v", len(objs), objs)
	}

	got := make([]string, len(objs))
	for i, o := range objs {
		got[i] = o.Path
	}
	sort.Strings(got)
	want := []string{"/a.txt", "/sub/b.txt", "/sub/deep/c.txt"}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("object[%d]: got %q, want %q", i, got[i], w)
		}
	}

	// At minimum there should be one progress call per committed file.
	if progressCalls < 3 {
		t.Errorf("expected at least 3 progress calls, got %d", progressCalls)
	}
}

func TestCreateObjects_skipUnchanged(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	memFS := fstest.MapFS{
		"x.txt": {Data: []byte("unchanged content")},
		"y.txt": {Data: []byte("also unchanged")},
	}

	// First upload — no options, both files should be committed.
	objs, err := c.CreateObjects(context.Background(), "testbucket", memFS)
	if err != nil {
		t.Fatalf("first CreateObjects: %v", err)
	}
	if len(objs) != 2 {
		t.Fatalf("first upload: expected 2 objects, got %d: %+v", len(objs), objs)
	}

	// Second upload — SkipUnchanged is the default, sizes match the remote,
	// so both files should be skipped and the result should be empty.
	var uploadCalls int
	progress := func(_, _ int, _ string, _, _ int64) { uploadCalls++ }
	objs2, err := c.CreateObjects(context.Background(), "testbucket", memFS,
		httpclient.WithProgress(progress),
	)
	if err != nil {
		t.Fatalf("second CreateObjects: %v", err)
	}
	if len(objs2) != 0 {
		t.Errorf("expected 0 objects on second upload (all skipped), got %d: %+v", len(objs2), objs2)
	}
	if uploadCalls != 0 {
		t.Errorf("expected 0 progress calls (all skipped), got %d", uploadCalls)
	}
}

// TestCreateObjects_withPrefix verifies that WithPrefix stores objects under
// the specified remote path prefix.
func TestCreateObjects_withPrefix(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	memFS := fstest.MapFS{
		"img.png": {Data: []byte("png data")},
	}

	objs, err := c.CreateObjects(context.Background(), "testbucket", memFS,
		httpclient.WithPrefix("archive/2026"),
	)
	if err != nil {
		t.Fatalf("CreateObjects: %v", err)
	}
	if len(objs) != 1 {
		t.Fatalf("expected 1 object, got %d: %+v", len(objs), objs)
	}
	want := "/archive/2026/img.png"
	if objs[0].Path != want {
		t.Errorf("path: got %q, want %q", objs[0].Path, want)
	}
}

// TestCreateObjects_withCheckNil verifies that WithCheck(nil) disables the
// default skip-unchanged behaviour, causing files to be re-uploaded even when
// size and modtime match.
func TestCreateObjects_withCheckNil(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	memFS := fstest.MapFS{
		"z.txt": {Data: []byte("same content")},
	}

	// First upload.
	if _, err := c.CreateObjects(context.Background(), "testbucket", memFS); err != nil {
		t.Fatalf("first upload: %v", err)
	}

	// Second upload with no skip check — must upload again.
	objs2, err := c.CreateObjects(context.Background(), "testbucket", memFS,
		httpclient.WithCheck(nil),
	)
	if err != nil {
		t.Fatalf("second upload: %v", err)
	}
	if len(objs2) != 1 {
		t.Errorf("expected 1 object re-uploaded, got %d", len(objs2))
	}
}

// TestCreateObjects_skipChanged verifies that files whose size has changed are
// NOT skipped by the default skipUnchanged check (size-mismatch branch).
func TestCreateObjects_skipChanged(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	memFS := fstest.MapFS{
		"data.txt": {Data: []byte("short")},
	}

	// First upload.
	if _, err := c.CreateObjects(context.Background(), "testbucket", memFS); err != nil {
		t.Fatalf("first upload: %v", err)
	}

	// Grow the file — size no longer matches remote.
	memFS["data.txt"] = &fstest.MapFile{Data: []byte("much longer content now")}

	// Second upload — size differs, so the file must be uploaded again.
	var progressCalls int
	objs2, err := c.CreateObjects(context.Background(), "testbucket", memFS,
		httpclient.WithProgress(func(_, _ int, _ string, _, _ int64) { progressCalls++ }),
	)
	if err != nil {
		t.Fatalf("second upload: %v", err)
	}
	if len(objs2) != 1 {
		t.Errorf("expected 1 re-uploaded object (size changed), got %d: %+v", len(objs2), objs2)
	}
}

// TestCreateObjects_skipUnchangedModtime exercises the skipUnchanged branch where
// both local and remote modtimes are non-zero (lmt.Equal(rmt) comparison).
func TestCreateObjects_skipUnchangedModtime(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	mt := time.Unix(1_000_000, 0).UTC()
	memFS := fstest.MapFS{
		"timed.txt": {Data: []byte("timed content"), ModTime: mt},
	}

	// First upload — Last-Modified is sent; server stores the modtime.
	if _, err := c.CreateObjects(context.Background(), "testbucket", memFS); err != nil {
		t.Fatalf("first upload: %v", err)
	}

	// Second upload — size and modtime both match; skipUnchanged returns true.
	objs2, err := c.CreateObjects(context.Background(), "testbucket", memFS)
	if err != nil {
		t.Fatalf("second upload: %v", err)
	}
	if len(objs2) != 0 {
		t.Errorf("expected 0 objects (same modtime, skipped), got %d", len(objs2))
	}

	// Third upload — modtime changed; skipUnchanged returns false.
	memFS["timed.txt"] = &fstest.MapFile{Data: []byte("timed content"), ModTime: mt.Add(time.Hour)}
	objs3, err := c.CreateObjects(context.Background(), "testbucket", memFS)
	if err != nil {
		t.Fatalf("third upload: %v", err)
	}
	if len(objs3) != 1 {
		t.Errorf("expected 1 object (modtime changed), got %d", len(objs3))
	}
}
