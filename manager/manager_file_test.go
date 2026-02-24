package manager

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	// Packages
	backend "github.com/mutablelogic/go-filer/backend"
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////
// FILE BACKEND - MANAGER LIFECYCLE TESTS

func Test_ManagerFile_New(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	tmpDir := t.TempDir()

	mgr, err := New(ctx, WithBackend(ctx, "file://testfiles"+tmpDir, backend.WithCreateDir()))
	assert.NoError(err)
	assert.NotNil(mgr)
	defer mgr.Close()
}

func Test_ManagerFile_MultipleBackends(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	mgr, err := New(ctx,
		WithBackend(ctx, "file://testfiles1"+tmpDir1, backend.WithCreateDir()),
		WithBackend(ctx, "file://testfiles2"+tmpDir2, backend.WithCreateDir()),
	)
	assert.NoError(err)
	assert.NotNil(mgr)
	defer mgr.Close()

	backends := mgr.Backends()
	assert.Len(backends, 2)
}

func Test_ManagerFile_Close(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	tmpDir := t.TempDir()

	mgr, err := New(ctx, WithBackend(ctx, "file://testfiles"+tmpDir, backend.WithCreateDir()))
	assert.NoError(err)

	err = mgr.Close()
	assert.NoError(err)
}

////////////////////////////////////////////////////////////////////////////////
// FILE BACKEND - KEY ROUTING TESTS

func Test_ManagerFile_Key(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	tmpDir := t.TempDir()

	mgr, err := New(ctx, WithBackend(ctx, "file://testfiles"+tmpDir, backend.WithCreateDir()))
	assert.NoError(err)
	defer mgr.Close()

	// Matching backend and path
	assert.Equal("/somefile.txt", mgr.Key("testfiles", "/somefile.txt"))

	// No backend with this name
	assert.Equal("", mgr.Key("other", "/somefile.txt"))
}

////////////////////////////////////////////////////////////////////////////////
// FILE BACKEND - BACKEND ROUTING ERROR TESTS

func Test_ManagerFile_NoBackendError(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	tmpDir := t.TempDir()

	mgr, err := New(ctx, WithBackend(ctx, "file://testfiles"+tmpDir, backend.WithCreateDir()))
	assert.NoError(err)
	defer mgr.Close()

	// GetObject with wrong backend
	_, err = mgr.GetObject(ctx, schema.GetObjectRequest{Name: "nomatch", Path: "/file.txt"})
	assert.Error(err)
	assert.Contains(err.Error(), "no backend found")

	// ListObjects with wrong backend
	_, err = mgr.ListObjects(ctx, schema.ListObjectsRequest{Name: "nomatch", Path: "/"})
	assert.Error(err)

	// DeleteObject with wrong backend
	_, err = mgr.DeleteObject(ctx, schema.DeleteObjectRequest{Name: "nomatch", Path: "/file.txt"})
	assert.Error(err)

	// CreateObject with wrong backend
	_, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		Name: "nomatch",

		Path: "/file.txt",
		Body: strings.NewReader("content"),
	})
	assert.Error(err)

	// ReadObject with wrong backend
	_, _, err = mgr.ReadObject(ctx, schema.ReadObjectRequest{Name: "nomatch", Path: "/file.txt"})
	assert.Error(err)
}

////////////////////////////////////////////////////////////////////////////////
// FILE BACKEND - CREATEOBJECT TESTS

func Test_ManagerFile_CreateObject(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	mgr, err := New(ctx, WithBackend(ctx, "file://testfiles"+tmpDir, backend.WithCreateDir()))
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("create simple object", func(t *testing.T) {
		assert := assert.New(t)

		content := "hello from file manager"
		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			Name:        "testfiles",
			Path:        "/created.txt",
			Body:        strings.NewReader(content),
			ContentType: "text/plain",
		})
		assert.NoError(err)
		assert.Equal("testfiles", obj.Name)
		assert.Equal("/created.txt", obj.Path)
		assert.Equal(int64(len(content)), obj.Size)
	})

	t.Run("create nested object", func(t *testing.T) {
		assert := assert.New(t)

		content := "nested content"
		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			Name:        "testfiles",
			Path:        "/subdir/nested/file.txt",
			Body:        strings.NewReader(content),
			ContentType: "text/plain",
		})
		assert.NoError(err)
		assert.Equal("testfiles", obj.Name)
		assert.Equal("/subdir/nested/file.txt", obj.Path)
		assert.Equal(int64(len(content)), obj.Size)
	})

	t.Run("create with metadata", func(t *testing.T) {
		assert := assert.New(t)

		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			Name:        "testfiles",
			Path:        "/with-meta.txt",
			Body:        strings.NewReader("metadata test"),
			ContentType: "text/plain",
			Meta:        schema.ObjectMeta{"author": "test", "version": "1"},
		})
		assert.NoError(err)
		assert.Equal("test", obj.Meta["author"])
		assert.Equal("1", obj.Meta["version"])
	})

	t.Run("overwrite existing object", func(t *testing.T) {
		assert := assert.New(t)

		// Create initial
		_, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			Name: "testfiles",
			Path: "/overwrite.txt",
			Body: strings.NewReader("original"),
		})
		assert.NoError(err)

		// Overwrite
		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			Name: "testfiles",
			Path: "/overwrite.txt",
			Body: strings.NewReader("new content"),
		})
		assert.NoError(err)
		assert.Equal(int64(len("new content")), obj.Size)

		// Verify new content
		reader, _, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{Name: "testfiles", Path: "/overwrite.txt"})
		assert.NoError(err)
		defer reader.Close()
		data, _ := io.ReadAll(reader)
		assert.Equal("new content", string(data))
	})
}

////////////////////////////////////////////////////////////////////////////////
// FILE BACKEND - READOBJECT TESTS

func Test_ManagerFile_ReadObject(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	mgr, err := New(ctx, WithBackend(ctx, "file://testfiles"+tmpDir, backend.WithCreateDir()))
	require.NoError(t, err)
	defer mgr.Close()

	// Create a test object
	content := "read me via file manager"
	_, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		Name:        "testfiles",
		Path:        "/readable.txt",
		Body:        strings.NewReader(content),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	t.Run("read existing object", func(t *testing.T) {
		assert := assert.New(t)

		reader, obj, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{Name: "testfiles", Path: "/readable.txt"})
		assert.NoError(err)
		defer reader.Close()

		assert.Equal("testfiles", obj.Name)
		assert.Equal("/readable.txt", obj.Path)
		assert.Equal(int64(len(content)), obj.Size)

		data, err := io.ReadAll(reader)
		assert.NoError(err)
		assert.Equal(content, string(data))
	})

	t.Run("read non-existent object", func(t *testing.T) {
		assert := assert.New(t)

		_, _, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{Name: "testfiles", Path: "/notfound.txt"})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})
}

////////////////////////////////////////////////////////////////////////////////
// FILE BACKEND - GETOBJECT TESTS

func Test_ManagerFile_GetObject(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	mgr, err := New(ctx, WithBackend(ctx, "file://testfiles"+tmpDir, backend.WithCreateDir()))
	require.NoError(t, err)
	defer mgr.Close()

	// Create a test object
	content := "get my metadata"
	_, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		Name:        "testfiles",
		Path:        "/getme.txt",
		Body:        strings.NewReader(content),
		ContentType: "text/plain",
		Meta:        schema.ObjectMeta{"key": "value"},
	})
	require.NoError(t, err)

	t.Run("get existing object metadata", func(t *testing.T) {
		assert := assert.New(t)

		obj, err := mgr.GetObject(ctx, schema.GetObjectRequest{Name: "testfiles", Path: "/getme.txt"})
		assert.NoError(err)
		assert.Equal("testfiles", obj.Name)
		assert.Equal("/getme.txt", obj.Path)
		assert.Equal(int64(len(content)), obj.Size)
	})

	t.Run("get non-existent object", func(t *testing.T) {
		assert := assert.New(t)

		_, err := mgr.GetObject(ctx, schema.GetObjectRequest{Name: "testfiles", Path: "/notfound.txt"})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})
}

////////////////////////////////////////////////////////////////////////////////
// FILE BACKEND - LISTOBJECTS TESTS

func Test_ManagerFile_ListObjects(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	mgr, err := New(ctx, WithBackend(ctx, "file://testfiles"+tmpDir, backend.WithCreateDir()))
	require.NoError(t, err)
	defer mgr.Close()

	// Create test structure
	files := []string{
		"file1.txt",
		"file2.txt",
		"subdir/nested1.txt",
		"subdir/nested2.txt",
		"subdir/deep/file.txt",
	}
	for _, f := range files {
		_, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			Name:        "testfiles",
			Path:        "/" + f,
			Body:        strings.NewReader("content of " + f),
			ContentType: "text/plain",
		})
		require.NoError(t, err)
	}

	t.Run("list root non-recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			Name:      "testfiles",
			Path:      "/",
			Recursive: false,
		})
		assert.NoError(err)
		assert.Equal("testfiles", resp.Name)
		// Should have file1.txt, file2.txt, and subdir/ marker
		assert.GreaterOrEqual(len(resp.Body), 2)
	})

	t.Run("list root recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			Name:      "testfiles",
			Path:      "/",
			Recursive: true,
		})
		assert.NoError(err)
		assert.Len(resp.Body, 5)
	})

	t.Run("list prefix non-recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			Name:      "testfiles",
			Path:      "/subdir/",
			Recursive: false,
		})
		assert.NoError(err)
		// Should have nested1.txt, nested2.txt, and deep/ marker
		assert.GreaterOrEqual(len(resp.Body), 2)
	})

	t.Run("list prefix recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			Name:      "testfiles",
			Path:      "/subdir/",
			Recursive: true,
		})
		assert.NoError(err)
		assert.Len(resp.Body, 3)
	})

	t.Run("get single object by URL", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testfiles",
			Path: "/file1.txt",
		})
		assert.NoError(err)
		assert.Len(resp.Body, 1)
		assert.Equal("testfiles", resp.Body[0].Name)
		assert.Equal("/file1.txt", resp.Body[0].Path)
	})

	t.Run("list empty prefix", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			Name: "testfiles",
			Path: "/nonexistent/",
		})
		assert.NoError(err)
		assert.Len(resp.Body, 0)
	})
}

////////////////////////////////////////////////////////////////////////////////
// FILE BACKEND - DELETEOBJECT TESTS

func Test_ManagerFile_DeleteObject(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	mgr, err := New(ctx, WithBackend(ctx, "file://testfiles"+tmpDir, backend.WithCreateDir()))
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("delete existing object", func(t *testing.T) {
		assert := assert.New(t)

		// Create object
		content := "delete me"
		_, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			Name: "testfiles",
			Path: "/deleteme.txt",
			Body: strings.NewReader(content),
		})
		assert.NoError(err)

		// Delete it
		obj, err := mgr.DeleteObject(ctx, schema.DeleteObjectRequest{Name: "testfiles", Path: "/deleteme.txt"})
		assert.NoError(err)
		assert.Equal("testfiles", obj.Name)
		assert.Equal("/deleteme.txt", obj.Path)
		assert.Equal(int64(len(content)), obj.Size)

		// Verify it's gone
		_, err = mgr.GetObject(ctx, schema.GetObjectRequest{Name: "testfiles", Path: "/deleteme.txt"})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})

	t.Run("delete non-existent object", func(t *testing.T) {
		assert := assert.New(t)

		_, err := mgr.DeleteObject(ctx, schema.DeleteObjectRequest{Name: "testfiles", Path: "/notfound.txt"})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})

	t.Run("delete nested object", func(t *testing.T) {
		assert := assert.New(t)

		// Create nested object
		_, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			Name: "testfiles",
			Path: "/nested/deep/file.txt",
			Body: strings.NewReader("nested content"),
		})
		assert.NoError(err)

		// Delete it
		_, err = mgr.DeleteObject(ctx, schema.DeleteObjectRequest{Name: "testfiles", Path: "/nested/deep/file.txt"})
		assert.NoError(err)

		// Verify it's gone
		_, err = mgr.GetObject(ctx, schema.GetObjectRequest{Name: "testfiles", Path: "/nested/deep/file.txt"})
		assert.Error(err)
	})
}

////////////////////////////////////////////////////////////////////////////////
// FILE BACKEND - FULL WORKFLOW TESTS

func Test_ManagerFile_FullWorkflow(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	tmpDir := t.TempDir()

	mgr, err := New(ctx, WithBackend(ctx, "file://testfiles"+tmpDir, backend.WithCreateDir()))
	require.NoError(t, err)
	defer mgr.Close()

	// 1. Create an object
	content := "full workflow test content"
	createdObj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
		Name:        "testfiles",
		Path:        "/workflow/test.txt",
		Body:        strings.NewReader(content),
		ContentType: "text/plain",
		Meta:        schema.ObjectMeta{"step": "created"},
	})
	assert.NoError(err)
	assert.Equal("testfiles", createdObj.Name)
	assert.Equal("/workflow/test.txt", createdObj.Path)

	// 2. Get object metadata
	gotObj, err := mgr.GetObject(ctx, schema.GetObjectRequest{Name: "testfiles", Path: "/workflow/test.txt"})
	assert.NoError(err)
	assert.Equal(int64(len(content)), gotObj.Size)

	// 3. Read object content
	reader, readObj, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{Name: "testfiles", Path: "/workflow/test.txt"})
	assert.NoError(err)
	data, _ := io.ReadAll(reader)
	reader.Close()
	assert.Equal(content, string(data))
	assert.Equal(int64(len(content)), readObj.Size)

	// 4. List objects
	listResp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{Name: "testfiles", Path: "/workflow/"})
	assert.NoError(err)
	assert.Len(listResp.Body, 1)

	// 5. Delete object
	deletedObj, err := mgr.DeleteObject(ctx, schema.DeleteObjectRequest{Name: "testfiles", Path: "/workflow/test.txt"})
	assert.NoError(err)
	assert.Equal("testfiles", deletedObj.Name)
	assert.Equal("/workflow/test.txt", deletedObj.Path)

	// 6. Verify object is gone
	_, err = mgr.GetObject(ctx, schema.GetObjectRequest{Name: "testfiles", Path: "/workflow/test.txt"})
	assert.Error(err)

	// 7. Verify list is empty
	listResp, err = mgr.ListObjects(ctx, schema.ListObjectsRequest{Name: "testfiles", Path: "/workflow/"})
	assert.NoError(err)
	assert.Len(listResp.Body, 0)
}

////////////////////////////////////////////////////////////////////////////////
// FILE BACKEND - EDGE CASES

func Test_ManagerFile_EdgeCases(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	mgr, err := New(ctx, WithBackend(ctx, "file://testfiles"+tmpDir, backend.WithCreateDir()))
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("empty content", func(t *testing.T) {
		assert := assert.New(t)

		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			Name: "testfiles",
			Path: "/empty.txt",
			Body: strings.NewReader(""),
		})
		assert.NoError(err)
		assert.Equal(int64(0), obj.Size)

		// Should be retrievable
		reader, _, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{Name: "testfiles", Path: "/empty.txt"})
		assert.NoError(err)
		data, _ := io.ReadAll(reader)
		reader.Close()
		assert.Equal("", string(data))
	})

	t.Run("binary content", func(t *testing.T) {
		assert := assert.New(t)

		binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			Name:        "testfiles",
			Path:        "/binary.bin",
			Body:        bytes.NewReader(binaryData),
			ContentType: "application/octet-stream",
		})
		assert.NoError(err)
		assert.Equal(int64(len(binaryData)), obj.Size)

		// Should be retrievable
		reader, _, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{Name: "testfiles", Path: "/binary.bin"})
		assert.NoError(err)
		data, _ := io.ReadAll(reader)
		reader.Close()
		assert.Equal(binaryData, data)
	})

	t.Run("unicode content", func(t *testing.T) {
		assert := assert.New(t)

		unicodeContent := "Hello 世界! 🌍 Привет мир"
		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			Name: "testfiles",
			Path: "/unicode.txt",
			Body: strings.NewReader(unicodeContent),
		})
		assert.NoError(err)
		assert.Equal(int64(len(unicodeContent)), obj.Size)

		// Should be retrievable
		reader, _, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{Name: "testfiles", Path: "/unicode.txt"})
		assert.NoError(err)
		data, _ := io.ReadAll(reader)
		reader.Close()
		assert.Equal(unicodeContent, string(data))
	})
}
