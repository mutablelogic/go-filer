package file_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	harness "github.com/mutablelogic/go-filer/backend/test"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

func TestMain(m *testing.M) {
	harness.Main(m)
}

func createObject(t *testing.T, backend interface {
	CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error)
}, ctx context.Context, p string, body []byte) *schema.Object {
	t.Helper()
	obj, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
		ObjectKey: schema.ObjectKey{Path: p},
		Body:      bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("CreateObject(%q): %v", p, err)
	}
	return obj
}

///////////////////////////////////////////////////////////////////////////////
// CreateObject

func TestCreateObject_001(t *testing.T) {
	backend := harness.BeginFile(t)
	defer harness.End(t, backend)
	ctx := context.Background()

	t.Run("returns-metadata", func(t *testing.T) {
		body := []byte("hello world")
		obj := createObject(t, backend, ctx, "meta.txt", body)
		if obj.Volume != backend.Name() {
			t.Errorf("Volume: got %q, want %q", obj.Volume, backend.Name())
		}
		if obj.Path != "meta.txt" {
			t.Errorf("Path: got %q, want %q", obj.Path, "meta.txt")
		}
		if obj.Size != int64(len(body)) {
			t.Errorf("Size: got %d, want %d", obj.Size, len(body))
		}
		if obj.ModTime.IsZero() {
			t.Error("ModTime should not be zero")
		}
		if obj.ContentType == "" {
			t.Error("ContentType should not be empty")
		}
		if obj.IsDir {
			t.Error("IsDir should be false for a file")
		}
	})

	t.Run("nil-body-creates-empty-file", func(t *testing.T) {
		obj, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "empty.txt"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if obj.Size != 0 {
			t.Errorf("Size: got %d, want 0", obj.Size)
		}
	})

	t.Run("if-not-exists-conflict", func(t *testing.T) {
		createObject(t, backend, ctx, "conflict.txt", []byte("original"))
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			ObjectKey:   schema.ObjectKey{Path: "conflict.txt"},
			IfNotExists: true,
			Body:        bytes.NewReader([]byte("new")),
		})
		if !errors.Is(err, gofiler.ErrConflict) {
			t.Errorf("expected ErrConflict, got %v", err)
		}
	})

	t.Run("overwrites-existing-file", func(t *testing.T) {
		createObject(t, backend, ctx, "overwrite.txt", []byte("original"))
		obj := createObject(t, backend, ctx, "overwrite.txt", []byte("new"))
		if obj.Size != int64(len("new")) {
			t.Errorf("Size: got %d, want %d", obj.Size, len("new"))
		}
	})

	t.Run("path-is-directory", func(t *testing.T) {
		createObject(t, backend, ctx, "subdir/file.txt", nil)
		_, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "subdir"},
		})
		if !errors.Is(err, gofiler.ErrBadParameter) {
			t.Errorf("expected ErrBadParameter, got %v", err)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// GetObject

func TestGetObject_001(t *testing.T) {
	backend := harness.BeginFile(t)
	defer harness.End(t, backend)
	ctx := context.Background()

	t.Run("not-found", func(t *testing.T) {
		_, err := backend.GetObject(ctx, schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "missing.txt"},
		})
		if !errors.Is(err, gofiler.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("path-is-directory", func(t *testing.T) {
		createObject(t, backend, ctx, "subdir/file.txt", nil)
		_, err := backend.GetObject(ctx, schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "subdir"},
		})
		if !errors.Is(err, gofiler.ErrBadParameter) {
			t.Errorf("expected ErrBadParameter, got %v", err)
		}
	})

	t.Run("returns-metadata", func(t *testing.T) {
		body := []byte("hello world")
		createObject(t, backend, ctx, "meta.txt", body)
		obj, err := backend.GetObject(ctx, schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "meta.txt"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if obj.Volume != backend.Name() {
			t.Errorf("Volume: got %q, want %q", obj.Volume, backend.Name())
		}
		if obj.Path != "meta.txt" {
			t.Errorf("Path: got %q, want %q", obj.Path, "meta.txt")
		}
		if obj.Size != int64(len(body)) {
			t.Errorf("Size: got %d, want %d", obj.Size, len(body))
		}
		if obj.ModTime.IsZero() {
			t.Error("ModTime should not be zero")
		}
		if obj.ContentType == "" {
			t.Error("ContentType should not be empty")
		}
		if obj.IsDir {
			t.Error("IsDir should be false")
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// ReadObject

func TestReadObject_001(t *testing.T) {
	backend := harness.BeginFile(t)
	defer harness.End(t, backend)
	ctx := context.Background()

	t.Run("not-found", func(t *testing.T) {
		_, _, err := backend.ReadObject(ctx, schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "missing.txt"},
		})
		if !errors.Is(err, gofiler.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("path-is-directory", func(t *testing.T) {
		createObject(t, backend, ctx, "subdir/file.txt", nil)
		_, _, err := backend.ReadObject(ctx, schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "subdir"},
		})
		if !errors.Is(err, gofiler.ErrBadParameter) {
			t.Errorf("expected ErrBadParameter, got %v", err)
		}
	})

	t.Run("reads-content", func(t *testing.T) {
		body := []byte("hello from ReadObject")
		createObject(t, backend, ctx, "read.txt", body)

		rc, obj, err := backend.ReadObject(ctx, schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "read.txt"},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()

		got, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(body) {
			t.Errorf("content: got %q, want %q", got, body)
		}
		if obj.Size != int64(len(body)) {
			t.Errorf("Size: got %d, want %d", obj.Size, len(body))
		}
		if obj.ContentType == "" {
			t.Error("ContentType should not be empty")
		}
		if obj.Path != "read.txt" {
			t.Errorf("Path: got %q, want %q", obj.Path, "read.txt")
		}
		if obj.Volume != backend.Name() {
			t.Errorf("Volume: got %q, want %q", obj.Volume, backend.Name())
		}
	})

	t.Run("reader-is-closeable", func(t *testing.T) {
		createObject(t, backend, ctx, "close.txt", []byte("x"))
		rc, _, err := backend.ReadObject(ctx, schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "close.txt"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := rc.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// DeleteObjects

func TestDeleteObjects_001(t *testing.T) {
	backend := harness.BeginFile(t)
	defer harness.End(t, backend)
	ctx := context.Background()

	t.Run("not-found", func(t *testing.T) {
		err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			ObjectKey: schema.ObjectKey{Path: "missing.txt"},
		})
		if !errors.Is(err, gofiler.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("deletes-file", func(t *testing.T) {
		createObject(t, backend, ctx, "todelete.txt", []byte("bye"))

		if err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			ObjectKey: schema.ObjectKey{Path: "todelete.txt"},
		}); err != nil {
			t.Fatalf("DeleteObjects: %v", err)
		}

		_, err := backend.GetObject(ctx, schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Path: "todelete.txt"},
		})
		if !errors.Is(err, gofiler.ErrNotFound) {
			t.Errorf("expected ErrNotFound after delete, got %v", err)
		}
	})

	t.Run("deletes-directory-and-contents", func(t *testing.T) {
		for _, p := range []string{"subtree/a.txt", "subtree/b/c.txt"} {
			createObject(t, backend, ctx, p, []byte("x"))
		}

		if err := backend.DeleteObjects(ctx, schema.DeleteObjectsRequest{
			ObjectKey: schema.ObjectKey{Path: "subtree"},
		}); err != nil {
			t.Fatalf("DeleteObjects: %v", err)
		}

		// All paths under subtree should be gone
		for _, p := range []string{"subtree/a.txt", "subtree/b/c.txt", "subtree"} {
			iter := &schema.ObjectListIterator{Recursive: true}
			if err := backend.ListObjects(ctx, iter); !errors.Is(err, io.EOF) {
				t.Fatalf("ListObjects: %v", err)
			}
			for _, obj := range iter.Body {
				if obj.Path == p {
					t.Errorf("path %q still present after DeleteObjects", p)
				}
			}
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// ListObjects — pagination

func TestListObjects_001(t *testing.T) {
	backend := harness.BeginFile(t)
	defer harness.End(t, backend)
	ctx := context.Background()

	// Create one more file than the default page size
	const total = schema.ObjectListLimit + 1
	for i := range total {
		createObject(t, backend, ctx, types.Stringify(i)+".txt", []byte("x"))
	}

	iterator := &schema.ObjectListIterator{}

	// First page: not the last, so nil is returned
	if err := backend.ListObjects(ctx, iterator); err != nil {
		t.Fatalf("page 1: expected nil, got %v", err)
	}
	if len(iterator.Body) != schema.ObjectListLimit {
		t.Errorf("page 1: expected %d objects, got %d", schema.ObjectListLimit, len(iterator.Body))
	}
	if iterator.Token == nil {
		t.Error("page 1: token should not be nil (more pages remain)")
	}

	// Second page: last page, so io.EOF is returned and token is cleared
	if err := backend.ListObjects(ctx, iterator); !errors.Is(err, io.EOF) {
		t.Fatalf("page 2: expected io.EOF, got %v", err)
	}
	if len(iterator.Body) != 1 {
		t.Errorf("page 2: expected 1 object, got %d", len(iterator.Body))
	}
	if iterator.Token != nil {
		t.Error("page 2: token should be nil after last page")
	}
}

///////////////////////////////////////////////////////////////////////////////
// ListObjects — hidden files

func TestListObjects_002(t *testing.T) {
	backend := harness.BeginFile(t)
	defer harness.End(t, backend)
	ctx := context.Background()

	for _, p := range []string{
		"visible.txt",
		".hidden.txt",
		"subdir/visible2.txt",
		".hiddendir/file.txt",
	} {
		createObject(t, backend, ctx, p, []byte("x"))
	}

	iterator := &schema.ObjectListIterator{Recursive: true}
	if err := backend.ListObjects(ctx, iterator); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	t.Logf("listed %d objects", len(iterator.Body))
	if len(iterator.Body) != 2 {
		t.Errorf("expected 2 visible files, got %d", len(iterator.Body))
	}
	for _, obj := range iterator.Body {
		t.Logf("  %q", obj.Path)
	}
}

///////////////////////////////////////////////////////////////////////////////
// ListObjects — subdirectory path

func TestListObjects_003(t *testing.T) {
	backend := harness.BeginFile(t)
	defer harness.End(t, backend)
	ctx := context.Background()

	for _, p := range []string{
		"root.txt",
		"sub/a.txt",
		"sub/b.txt",
		"other/c.txt",
	} {
		createObject(t, backend, ctx, p, []byte("x"))
	}

	// Listing from "sub" should only return files under "sub"
	iterator := &schema.ObjectListIterator{Path: types.Ptr("sub")}
	if err := backend.ListObjects(ctx, iterator); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	t.Logf("listed %d objects", len(iterator.Body))
	if len(iterator.Body) != 2 {
		t.Errorf("expected 2 objects under sub/, got %d", len(iterator.Body))
	}
	for _, obj := range iterator.Body {
		t.Logf("  %q", obj.Path)
	}
}

///////////////////////////////////////////////////////////////////////////////
// FileBackend

func TestFileBackend_001(t *testing.T) {
	backend := harness.BeginFile(t)
	defer harness.End(t, backend)
	t.Log("backend:", backend.Name(), "url:", backend.URL().String())
}

func TestFileBackend_002(t *testing.T) {
	backend := harness.BeginFile(t)
	defer harness.End(t, backend)

	ctx := context.Background()
	const count = 10

	// Create files spread across two subdirectories
	for i := range count {
		for _, dir := range []string{"a", "b"} {
			p := dir + "/" + types.Stringify(i) + ".txt"
			if _, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
				ObjectKey: schema.ObjectKey{Path: p},
				Body:      bytes.NewReader([]byte("Hello, world!")),
			}); err != nil {
				t.Fatalf("CreateObject(%q): %v", p, err)
			}
		}
	}

	// List all files recursively and verify the count
	iterator := &schema.ObjectListIterator{Recursive: true}
	if err := backend.ListObjects(ctx, iterator); !errors.Is(err, io.EOF) {
		t.Fatalf("ListObjects: expected io.EOF, got %v", err)
	}
	t.Logf("listed %d objects", len(iterator.Body))
	if len(iterator.Body) != count*2 {
		t.Errorf("expected %d objects, got %d", count*2, len(iterator.Body))
	}
	for _, obj := range iterator.Body {
		if obj.IsDir {
			t.Errorf("expected file, got directory: %q", obj.Path)
		}
	}
}

func TestFileBackend_003(t *testing.T) {
	backend := harness.BeginFile(t)
	defer harness.End(t, backend)

	ctx := context.Background()

	// Create files in three top-level subdirectories, with one nested subdir in "alpha"
	for _, p := range []string{
		"alpha/1.txt", "alpha/2.txt",
		"alpha/sub/3.txt",
		"beta/4.txt",
		"gamma/5.txt",
	} {
		if _, err := backend.CreateObject(ctx, schema.CreateObjectRequest{
			ObjectKey: schema.ObjectKey{Path: p},
			Body:      bytes.NewReader([]byte("content")),
		}); err != nil {
			t.Fatalf("CreateObject(%q): %v", p, err)
		}
	}

		// Path that does not exist
	t.Run("path-not-found", func(t *testing.T) {
		iterator := &schema.ObjectListIterator{Path: types.Ptr("doesnotexist")}
		if err := backend.ListObjects(ctx, iterator); !errors.Is(err, gofiler.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	// Path that is a file, not a directory
	t.Run("path-is-file", func(t *testing.T) {
		iterator := &schema.ObjectListIterator{Path: types.Ptr("alpha/1.txt")}
		if err := backend.ListObjects(ctx, iterator); !errors.Is(err, gofiler.ErrBadParameter) {
			t.Errorf("expected ErrBadParameter, got %v", err)
		}
	})

	// Path that is a directory
	t.Run("path-is-dir", func(t *testing.T) {
		iterator := &schema.ObjectListIterator{Path: types.Ptr("alpha")}
		if err := backend.ListObjects(ctx, iterator); !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
	})

	// Non-recursive: expect only the three top-level directories
	t.Run("non-recursive", func(t *testing.T) {
		iterator := &schema.ObjectListIterator{Type: types.Ptr(schema.ContentTypeDirectory)}
		if err := backend.ListObjects(ctx, iterator); !errors.Is(err, io.EOF) {
			t.Fatalf("ListObjects: expected io.EOF, got %v", err)
		}
		t.Logf("listed %d directories", len(iterator.Body))
		if len(iterator.Body) != 3 {
			t.Errorf("expected 3 directories, got %d", len(iterator.Body))
		}
		for _, obj := range iterator.Body {
			if !obj.IsDir {
				t.Errorf("expected directory, got file: %q", obj.Path)
			}
		}
	})

	// Recursive: expect all four directories (alpha, alpha/sub, beta, gamma)
	t.Run("recursive", func(t *testing.T) {
		iterator := &schema.ObjectListIterator{
			Type:      types.Ptr(schema.ContentTypeDirectory),
			Recursive: true,
		}
		if err := backend.ListObjects(ctx, iterator); !errors.Is(err, io.EOF) {
			t.Fatalf("ListObjects: expected io.EOF, got %v", err)
		}
		t.Logf("listed %d directories", len(iterator.Body))
		if len(iterator.Body) != 4 {
			t.Errorf("expected 4 directories, got %d", len(iterator.Body))
		}
		for _, obj := range iterator.Body {
			if !obj.IsDir {
				t.Errorf("expected directory, got file: %q", obj.Path)
			}
		}
	})
}
