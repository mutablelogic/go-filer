package httpclient_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

func TestListBackends(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	resp, err := c.ListBackends(context.Background())
	if err != nil {
		t.Fatalf("ListBackends: %v", err)
	}
	if len(resp.Body) == 0 {
		t.Fatal("expected at least one backend, got none")
	}
}

func TestListObjects_empty(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	resp, err := c.ListObjects(context.Background(), "testbucket", schema.ListObjectsRequest{Recursive: true})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("expected 0 objects on empty backend, got %d", resp.Count)
	}
}

func TestListObjects_afterUpload(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	paths := []string{"/list/x.txt", "/list/y.txt", "/list/nested/z.txt"}
	for _, p := range paths {
		if _, err := c.CreateObject(context.Background(), "testbucket", schema.CreateObjectRequest{
			Path: p, Body: strings.NewReader("data"), ContentType: "text/plain",
		}); err != nil {
			t.Fatalf("CreateObject %s: %v", p, err)
		}
	}

	resp, err := c.ListObjects(context.Background(), "testbucket", schema.ListObjectsRequest{
		Path: "/list", Recursive: true, Limit: 100,
	})
	if err != nil {
		t.Fatalf("ListObjects recursive: %v", err)
	}
	if resp.Count != 3 {
		t.Errorf("count: got %d, want 3", resp.Count)
	}
	if len(resp.Body) != 3 {
		t.Errorf("body len: got %d, want 3: %+v", len(resp.Body), resp.Body)
	}

	resp2, err := c.ListObjects(context.Background(), "testbucket", schema.ListObjectsRequest{
		Path: "/list", Recursive: false, Limit: 100,
	})
	if err != nil {
		t.Fatalf("ListObjects non-recursive: %v", err)
	}
	for _, obj := range resp2.Body {
		// A directory entry "/list/nested" is a valid immediate child;
		// only flag paths that go deeper (i.e., contain a slash after "nested").
		if strings.Contains(obj.Path, "nested/") {
			t.Errorf("non-recursive list returned deep nested path %q", obj.Path)
		}
	}
}

func TestCreateObject_roundtrip(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	data := []byte("hello world")
	obj, err := c.CreateObject(context.Background(), "testbucket", schema.CreateObjectRequest{
		Path:        "/hello.txt",
		Body:        bytes.NewReader(data),
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("CreateObject: %v", err)
	}
	if obj.Path != "/hello.txt" {
		t.Errorf("path: got %q, want %q", obj.Path, "/hello.txt")
	}
	if obj.Size != int64(len(data)) {
		t.Errorf("size: got %d, want %d", obj.Size, len(data))
	}
}

// TestCreateObject_defaultContentType exercises the putPayload.Type() fallback
// that returns "application/octet-stream" when no ContentType is set.
func TestCreateObject_defaultContentType(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	data := []byte("binary blob")
	// ContentType deliberately omitted → putPayload.Type() must return "application/octet-stream".
	obj, err := c.CreateObject(context.Background(), "testbucket", schema.CreateObjectRequest{
		Path: "/blob.bin",
		Body: bytes.NewReader(data),
	})
	if err != nil {
		t.Fatalf("CreateObject: %v", err)
	}
	if obj.Size != int64(len(data)) {
		t.Errorf("size: got %d, want %d", obj.Size, len(data))
	}
}

func TestCreateObject_ifNotExists(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	req := schema.CreateObjectRequest{
		Path:        "/once.txt",
		Body:        strings.NewReader("first"),
		ContentType: "text/plain",
		IfNotExists: true,
	}
	if _, err := c.CreateObject(context.Background(), "testbucket", req); err != nil {
		t.Fatalf("first CreateObject: %v", err)
	}
	req.Body = strings.NewReader("second")
	if _, err := c.CreateObject(context.Background(), "testbucket", req); err == nil {
		t.Fatal("expected error on second CreateObject with IfNotExists, got nil")
	}
}

func TestGetObject(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	data := []byte("metadata check")
	if _, err := c.CreateObject(context.Background(), "testbucket", schema.CreateObjectRequest{
		Path:        "/meta.txt",
		Body:        bytes.NewReader(data),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("CreateObject: %v", err)
	}
	obj, err := c.GetObject(context.Background(), "testbucket", schema.GetObjectRequest{Path: "/meta.txt"})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if obj == nil {
		t.Fatal("GetObject returned nil object")
	}
	if obj.Size != int64(len(data)) {
		t.Errorf("size: got %d, want %d", obj.Size, len(data))
	}
}

func TestReadObject(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	data := []byte("read me back")
	if _, err := c.CreateObject(context.Background(), "testbucket", schema.CreateObjectRequest{
		Path:        "/read.txt",
		Body:        bytes.NewReader(data),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("CreateObject: %v", err)
	}
	var got []byte
	obj, err := c.ReadObject(context.Background(), "testbucket", schema.ReadObjectRequest{
		GetObjectRequest: schema.GetObjectRequest{Path: "/read.txt"},
	}, func(chunk []byte) error {
		got = append(got, chunk...)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("body: got %q, want %q", got, data)
	}
	if obj.Size != int64(len(data)) {
		t.Errorf("size: got %d, want %d", obj.Size, len(data))
	}
}

func TestDeleteObject(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	if _, err := c.CreateObject(context.Background(), "testbucket", schema.CreateObjectRequest{
		Path: "/delete_me.txt", Body: strings.NewReader("bye"), ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("CreateObject: %v", err)
	}
	obj, err := c.DeleteObject(context.Background(), "testbucket", schema.DeleteObjectRequest{Path: "/delete_me.txt"})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if obj == nil {
		t.Fatal("DeleteObject returned nil")
	}
	if _, err := c.GetObject(context.Background(), "testbucket", schema.GetObjectRequest{Path: "/delete_me.txt"}); err == nil {
		t.Error("expected error fetching deleted object, got nil")
	}
}

func TestDeleteObjects_recursive(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	for _, p := range []string{"/dir/a.txt", "/dir/b.txt", "/dir/sub/c.txt"} {
		if _, err := c.CreateObject(context.Background(), "testbucket", schema.CreateObjectRequest{
			Path: p, Body: strings.NewReader("x"), ContentType: "text/plain",
		}); err != nil {
			t.Fatalf("CreateObject %s: %v", p, err)
		}
	}
	resp, err := c.DeleteObjects(context.Background(), "testbucket", schema.DeleteObjectsRequest{
		Path:      "/dir",
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if len(resp.Body) != 3 {
		t.Errorf("expected 3 deleted objects, got %d: %+v", len(resp.Body), resp.Body)
	}
}

func TestDeleteObjects_nonRecursive(t *testing.T) {
	c, cleanup := newTestServer(t, "mem://testbucket")
	defer cleanup()

	for _, p := range []string{"/flat/a.txt", "/flat/b.txt", "/flat/sub/c.txt"} {
		if _, err := c.CreateObject(context.Background(), "testbucket", schema.CreateObjectRequest{
			Path: p, Body: strings.NewReader("x"), ContentType: "text/plain",
		}); err != nil {
			t.Fatalf("CreateObject %s: %v", p, err)
		}
	}
	resp, err := c.DeleteObjects(context.Background(), "testbucket", schema.DeleteObjectsRequest{
		Path:      "/flat",
		Recursive: false,
	})
	if err != nil {
		t.Fatalf("DeleteObjects non-recursive: %v", err)
	}
	for _, obj := range resp.Body {
		if strings.Contains(obj.Path, "/sub/") {
			t.Errorf("non-recursive delete removed nested object %q", obj.Path)
		}
	}
}
