package manager

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	// Packages
	backend "github.com/mutablelogic/go-filer/pkg/backend"
	schema "github.com/mutablelogic/go-filer/pkg/schema"
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
	_, err = mgr.GetObject(ctx, "nomatch", schema.GetObjectRequest{Path: "/file.txt"})
	assert.Error(err)
	assert.Contains(err.Error(), "no backend found")

	// ListObjects with wrong backend
	_, err = mgr.ListObjects(ctx, "nomatch", schema.ListObjectsRequest{Path: "/"})
	assert.Error(err)

	// DeleteObject with wrong backend
	_, err = mgr.DeleteObject(ctx, "nomatch", schema.DeleteObjectRequest{Path: "/file.txt"})
	assert.Error(err)

	// CreateObject with wrong backend
	_, err = mgr.CreateObject(ctx, "nomatch", schema.CreateObjectRequest{
		Path: "/file.txt",
		Body: strings.NewReader("content"),
	})
	assert.Error(err)

	// ReadObject with wrong backend
	_, _, err = mgr.ReadObject(ctx, "nomatch", schema.ReadObjectRequest{Path: "/file.txt"})
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
		obj, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
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
		obj, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
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

		obj, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
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
		_, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
			Path: "/overwrite.txt",
			Body: strings.NewReader("original"),
		})
		assert.NoError(err)

		// Overwrite
		obj, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
			Path: "/overwrite.txt",
			Body: strings.NewReader("new content"),
		})
		assert.NoError(err)
		assert.Equal(int64(len("new content")), obj.Size)

		// Verify new content
		reader, _, err := mgr.ReadObject(ctx, "testfiles", schema.ReadObjectRequest{Path: "/overwrite.txt"})
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
	_, err = mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
		Path:        "/readable.txt",
		Body:        strings.NewReader(content),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	t.Run("read existing object", func(t *testing.T) {
		assert := assert.New(t)

		reader, obj, err := mgr.ReadObject(ctx, "testfiles", schema.ReadObjectRequest{Path: "/readable.txt"})
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

		_, _, err := mgr.ReadObject(ctx, "testfiles", schema.ReadObjectRequest{Path: "/notfound.txt"})
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
	_, err = mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
		Path:        "/getme.txt",
		Body:        strings.NewReader(content),
		ContentType: "text/plain",
		Meta:        schema.ObjectMeta{"key": "value"},
	})
	require.NoError(t, err)

	t.Run("get existing object metadata", func(t *testing.T) {
		assert := assert.New(t)

		obj, err := mgr.GetObject(ctx, "testfiles", schema.GetObjectRequest{Path: "/getme.txt"})
		assert.NoError(err)
		assert.Equal("testfiles", obj.Name)
		assert.Equal("/getme.txt", obj.Path)
		assert.Equal(int64(len(content)), obj.Size)
	})

	t.Run("get non-existent object", func(t *testing.T) {
		assert := assert.New(t)

		_, err := mgr.GetObject(ctx, "testfiles", schema.GetObjectRequest{Path: "/notfound.txt"})
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
		_, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
			Path:        "/" + f,
			Body:        strings.NewReader("content of " + f),
			ContentType: "text/plain",
		})
		require.NoError(t, err)
	}

	t.Run("list root non-recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, "testfiles", schema.ListObjectsRequest{
			Path:      "/",
			Recursive: false,
			Limit:     schema.MaxListLimit,
		})
		assert.NoError(err)
		assert.Equal("testfiles", resp.Name)
		// Should have file1.txt, file2.txt, and subdir/ marker
		assert.GreaterOrEqual(len(resp.Body), 2)
	})

	t.Run("list root recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, "testfiles", schema.ListObjectsRequest{
			Path:      "/",
			Recursive: true,
			Limit:     schema.MaxListLimit,
		})
		assert.NoError(err)
		assert.Len(resp.Body, 5)
	})

	t.Run("list prefix non-recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, "testfiles", schema.ListObjectsRequest{
			Path:      "/subdir/",
			Recursive: false,
			Limit:     schema.MaxListLimit,
		})
		assert.NoError(err)
		// Should have nested1.txt, nested2.txt, and deep/ marker
		assert.GreaterOrEqual(len(resp.Body), 2)
	})

	t.Run("list prefix recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, "testfiles", schema.ListObjectsRequest{
			Path:      "/subdir/",
			Recursive: true,
			Limit:     schema.MaxListLimit,
		})
		assert.NoError(err)
		assert.Len(resp.Body, 3)
	})

	t.Run("get single object by URL", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, "testfiles", schema.ListObjectsRequest{
			Path:  "/file1.txt",
			Limit: schema.MaxListLimit,
		})
		assert.NoError(err)
		assert.Len(resp.Body, 1)
		assert.Equal("testfiles", resp.Body[0].Name)
		assert.Equal("/file1.txt", resp.Body[0].Path)
	})

	t.Run("list empty prefix", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, "testfiles", schema.ListObjectsRequest{
			Path:  "/nonexistent/",
			Limit: schema.MaxListLimit,
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
		_, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
			Path: "/deleteme.txt",
			Body: strings.NewReader(content),
		})
		assert.NoError(err)

		// Delete it
		obj, err := mgr.DeleteObject(ctx, "testfiles", schema.DeleteObjectRequest{Path: "/deleteme.txt"})
		assert.NoError(err)
		assert.Equal("testfiles", obj.Name)
		assert.Equal("/deleteme.txt", obj.Path)
		assert.Equal(int64(len(content)), obj.Size)

		// Verify it's gone
		_, err = mgr.GetObject(ctx, "testfiles", schema.GetObjectRequest{Path: "/deleteme.txt"})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})

	t.Run("delete non-existent object", func(t *testing.T) {
		assert := assert.New(t)

		_, err := mgr.DeleteObject(ctx, "testfiles", schema.DeleteObjectRequest{Path: "/notfound.txt"})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})

	t.Run("delete nested object", func(t *testing.T) {
		assert := assert.New(t)

		// Create nested object
		_, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
			Path: "/nested/deep/file.txt",
			Body: strings.NewReader("nested content"),
		})
		assert.NoError(err)

		// Delete it
		_, err = mgr.DeleteObject(ctx, "testfiles", schema.DeleteObjectRequest{Path: "/nested/deep/file.txt"})
		assert.NoError(err)

		// Verify it's gone
		_, err = mgr.GetObject(ctx, "testfiles", schema.GetObjectRequest{Path: "/nested/deep/file.txt"})
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
	createdObj, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
		Path:        "/workflow/test.txt",
		Body:        strings.NewReader(content),
		ContentType: "text/plain",
		Meta:        schema.ObjectMeta{"step": "created"},
	})
	assert.NoError(err)
	assert.Equal("testfiles", createdObj.Name)
	assert.Equal("/workflow/test.txt", createdObj.Path)

	// 2. Get object metadata
	gotObj, err := mgr.GetObject(ctx, "testfiles", schema.GetObjectRequest{Path: "/workflow/test.txt"})
	assert.NoError(err)
	assert.Equal(int64(len(content)), gotObj.Size)

	// 3. Read object content
	reader, readObj, err := mgr.ReadObject(ctx, "testfiles", schema.ReadObjectRequest{Path: "/workflow/test.txt"})
	assert.NoError(err)
	data, _ := io.ReadAll(reader)
	reader.Close()
	assert.Equal(content, string(data))
	assert.Equal(int64(len(content)), readObj.Size)

	// 4. List objects
	listResp, err := mgr.ListObjects(ctx, "testfiles", schema.ListObjectsRequest{Path: "/workflow/", Limit: schema.MaxListLimit})
	assert.NoError(err)
	assert.Len(listResp.Body, 1)

	// 5. Delete object
	deletedObj, err := mgr.DeleteObject(ctx, "testfiles", schema.DeleteObjectRequest{Path: "/workflow/test.txt"})
	assert.NoError(err)
	assert.Equal("testfiles", deletedObj.Name)
	assert.Equal("/workflow/test.txt", deletedObj.Path)

	// 6. Verify object is gone
	_, err = mgr.GetObject(ctx, "testfiles", schema.GetObjectRequest{Path: "/workflow/test.txt"})
	assert.Error(err)

	// 7. Verify list is empty
	listResp, err = mgr.ListObjects(ctx, "testfiles", schema.ListObjectsRequest{Path: "/workflow/", Limit: schema.MaxListLimit})
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

		obj, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
			Path: "/empty.txt",
			Body: strings.NewReader(""),
		})
		assert.NoError(err)
		assert.Equal(int64(0), obj.Size)

		// Should be retrievable
		reader, _, err := mgr.ReadObject(ctx, "testfiles", schema.ReadObjectRequest{Path: "/empty.txt"})
		assert.NoError(err)
		data, _ := io.ReadAll(reader)
		reader.Close()
		assert.Equal("", string(data))
	})

	t.Run("binary content", func(t *testing.T) {
		assert := assert.New(t)

		binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
		obj, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
			Path:        "/binary.bin",
			Body:        bytes.NewReader(binaryData),
			ContentType: "application/octet-stream",
		})
		assert.NoError(err)
		assert.Equal(int64(len(binaryData)), obj.Size)

		// Should be retrievable
		reader, _, err := mgr.ReadObject(ctx, "testfiles", schema.ReadObjectRequest{Path: "/binary.bin"})
		assert.NoError(err)
		data, _ := io.ReadAll(reader)
		reader.Close()
		assert.Equal(binaryData, data)
	})

	t.Run("unicode content", func(t *testing.T) {
		assert := assert.New(t)

		unicodeContent := "Hello 世界! 🌍 Привет мир"
		obj, err := mgr.CreateObject(ctx, "testfiles", schema.CreateObjectRequest{
			Path: "/unicode.txt",
			Body: strings.NewReader(unicodeContent),
		})
		assert.NoError(err)
		assert.Equal(int64(len(unicodeContent)), obj.Size)

		// Should be retrievable
		reader, _, err := mgr.ReadObject(ctx, "testfiles", schema.ReadObjectRequest{Path: "/unicode.txt"})
		assert.NoError(err)
		data, _ := io.ReadAll(reader)
		reader.Close()
		assert.Equal(unicodeContent, string(data))
	})
}
