package manager

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"strings"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////
// MEM BACKEND - MANAGER LIFECYCLE TESTS

func Test_ManagerMem_New(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	mgr, err := New(ctx, WithBackend(ctx, "mem://testbucket"))
	assert.NoError(err)
	assert.NotNil(mgr)
	defer mgr.Close()
}

func Test_ManagerMem_MultipleBackends(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	mgr, err := New(ctx,
		WithBackend(ctx, "mem://bucket1"),
		WithBackend(ctx, "mem://bucket2"),
	)
	assert.NoError(err)
	assert.NotNil(mgr)
	defer mgr.Close()

	backends := mgr.Backends()
	assert.Len(backends, 2)
	assert.Contains(backends, "mem://bucket1")
	assert.Contains(backends, "mem://bucket2")
}

func Test_ManagerMem_Close(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	mgr, err := New(ctx, WithBackend(ctx, "mem://testbucket"))
	assert.NoError(err)

	err = mgr.Close()
	assert.NoError(err)
}

////////////////////////////////////////////////////////////////////////////////
// MEM BACKEND - KEY ROUTING TESTS

func Test_ManagerMem_Key(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	mgr, err := New(ctx, WithBackend(ctx, "mem://testbucket"))
	assert.NoError(err)
	defer mgr.Close()

	// Matching URL
	u, _ := url.Parse("mem://testbucket/somefile.txt")
	key := mgr.Key(u)
	assert.Equal("/somefile.txt", key)

	// Non-matching URL (wrong host)
	u2, _ := url.Parse("mem://otherbucket/somefile.txt")
	key2 := mgr.Key(u2)
	assert.Equal("", key2)

	// Non-matching URL (wrong scheme)
	u3, _ := url.Parse("file://testbucket/somefile.txt")
	key3 := mgr.Key(u3)
	assert.Equal("", key3)
}

func Test_ManagerMem_Key_MultipleBackends(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	mgr, err := New(ctx,
		WithBackend(ctx, "mem://files"),
		WithBackend(ctx, "mem://media"),
	)
	assert.NoError(err)
	defer mgr.Close()

	// First backend
	u1, _ := url.Parse("mem://files/doc.txt")
	assert.Equal("/doc.txt", mgr.Key(u1))

	// Second backend
	u2, _ := url.Parse("mem://media/video.mp4")
	assert.Equal("/video.mp4", mgr.Key(u2))

	// No match
	u3, _ := url.Parse("mem://other/file.txt")
	assert.Equal("", mgr.Key(u3))
}

////////////////////////////////////////////////////////////////////////////////
// MEM BACKEND - BACKEND ROUTING ERROR TESTS

func Test_ManagerMem_NoBackendError(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	mgr, err := New(ctx, WithBackend(ctx, "mem://testbucket"))
	assert.NoError(err)
	defer mgr.Close()

	// GetObject with wrong backend
	_, err = mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://other/file.txt"})
	assert.Error(err)
	assert.Contains(err.Error(), "no backend found")

	// ListObjects with wrong backend
	_, err = mgr.ListObjects(ctx, schema.ListObjectsRequest{URL: "mem://other/"})
	assert.Error(err)

	// DeleteObject with wrong backend
	_, err = mgr.DeleteObject(ctx, schema.DeleteObjectRequest{URL: "mem://other/file.txt"})
	assert.Error(err)

	// CreateObject with wrong backend
	_, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		URL:  "mem://other/file.txt",
		Body: strings.NewReader("content"),
	})
	assert.Error(err)

	// ReadObject with wrong backend
	_, _, err = mgr.ReadObject(ctx, schema.ReadObjectRequest{URL: "mem://other/file.txt"})
	assert.Error(err)
}

////////////////////////////////////////////////////////////////////////////////
// MEM BACKEND - CREATEOBJECT TESTS

func Test_ManagerMem_CreateObject(t *testing.T) {
	ctx := context.Background()

	mgr, err := New(ctx, WithBackend(ctx, "mem://testbucket"))
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("create simple object", func(t *testing.T) {
		assert := assert.New(t)

		content := "hello from manager"
		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         "mem://testbucket/created.txt",
			Body:        strings.NewReader(content),
			ContentType: "text/plain",
		})
		assert.NoError(err)
		assert.Equal("mem://testbucket/created.txt", obj.URL)
		assert.Equal(int64(len(content)), obj.Size)
		assert.Equal("text/plain", obj.ContentType)
	})

	t.Run("create nested object", func(t *testing.T) {
		assert := assert.New(t)

		content := "nested content"
		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         "mem://testbucket/subdir/nested/file.txt",
			Body:        strings.NewReader(content),
			ContentType: "text/plain",
		})
		assert.NoError(err)
		assert.Equal("mem://testbucket/subdir/nested/file.txt", obj.URL)
		assert.Equal(int64(len(content)), obj.Size)
	})

	t.Run("create with metadata", func(t *testing.T) {
		assert := assert.New(t)

		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         "mem://testbucket/with-meta.txt",
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
			URL:  "mem://testbucket/overwrite.txt",
			Body: strings.NewReader("original"),
		})
		assert.NoError(err)

		// Overwrite
		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			URL:  "mem://testbucket/overwrite.txt",
			Body: strings.NewReader("new content"),
		})
		assert.NoError(err)
		assert.Equal(int64(len("new content")), obj.Size)

		// Verify new content
		reader, _, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{URL: "mem://testbucket/overwrite.txt"})
		assert.NoError(err)
		defer reader.Close()
		data, _ := io.ReadAll(reader)
		assert.Equal("new content", string(data))
	})
}

////////////////////////////////////////////////////////////////////////////////
// MEM BACKEND - READOBJECT TESTS

func Test_ManagerMem_ReadObject(t *testing.T) {
	ctx := context.Background()

	mgr, err := New(ctx, WithBackend(ctx, "mem://testbucket"))
	require.NoError(t, err)
	defer mgr.Close()

	// Create a test object
	content := "read me via manager"
	_, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		URL:         "mem://testbucket/readable.txt",
		Body:        strings.NewReader(content),
		ContentType: "text/plain",
	})
	require.NoError(t, err)

	t.Run("read existing object", func(t *testing.T) {
		assert := assert.New(t)

		reader, obj, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{URL: "mem://testbucket/readable.txt"})
		assert.NoError(err)
		defer reader.Close()

		assert.Equal("mem://testbucket/readable.txt", obj.URL)
		assert.Equal(int64(len(content)), obj.Size)
		assert.Equal("text/plain", obj.ContentType)

		data, err := io.ReadAll(reader)
		assert.NoError(err)
		assert.Equal(content, string(data))
	})

	t.Run("read non-existent object", func(t *testing.T) {
		assert := assert.New(t)

		_, _, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{URL: "mem://testbucket/notfound.txt"})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})
}

////////////////////////////////////////////////////////////////////////////////
// MEM BACKEND - GETOBJECT TESTS

func Test_ManagerMem_GetObject(t *testing.T) {
	ctx := context.Background()

	mgr, err := New(ctx, WithBackend(ctx, "mem://testbucket"))
	require.NoError(t, err)
	defer mgr.Close()

	// Create a test object
	content := "get my metadata"
	_, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		URL:         "mem://testbucket/getme.txt",
		Body:        strings.NewReader(content),
		ContentType: "text/plain",
		Meta:        schema.ObjectMeta{"key": "value"},
	})
	require.NoError(t, err)

	t.Run("get existing object metadata", func(t *testing.T) {
		assert := assert.New(t)

		obj, err := mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/getme.txt"})
		assert.NoError(err)
		assert.Equal("mem://testbucket/getme.txt", obj.URL)
		assert.Equal(int64(len(content)), obj.Size)
		assert.Equal("text/plain", obj.ContentType)
		assert.Equal("value", obj.Meta["key"])
	})

	t.Run("get non-existent object", func(t *testing.T) {
		assert := assert.New(t)

		_, err := mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/notfound.txt"})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})
}

////////////////////////////////////////////////////////////////////////////////
// MEM BACKEND - LISTOBJECTS TESTS

func Test_ManagerMem_ListObjects(t *testing.T) {
	ctx := context.Background()

	mgr, err := New(ctx, WithBackend(ctx, "mem://testbucket"))
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
			URL:         "mem://testbucket/" + f,
			Body:        strings.NewReader("content of " + f),
			ContentType: "text/plain",
		})
		require.NoError(t, err)
	}

	t.Run("list root non-recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			URL:       "mem://testbucket/",
			Recursive: false,
		})
		assert.NoError(err)
		assert.Equal("mem://testbucket/", resp.URL)
		// Should have file1.txt, file2.txt, and subdir/ marker
		assert.GreaterOrEqual(len(resp.Body), 2)
	})

	t.Run("list root recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			URL:       "mem://testbucket/",
			Recursive: true,
		})
		assert.NoError(err)
		assert.Len(resp.Body, 5)
	})

	t.Run("list prefix non-recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			URL:       "mem://testbucket/subdir/",
			Recursive: false,
		})
		assert.NoError(err)
		// Should have nested1.txt, nested2.txt, and deep/ marker
		assert.GreaterOrEqual(len(resp.Body), 2)
	})

	t.Run("list prefix recursive", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			URL:       "mem://testbucket/subdir/",
			Recursive: true,
		})
		assert.NoError(err)
		assert.Len(resp.Body, 3)
	})

	t.Run("get single object by URL", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			URL: "mem://testbucket/file1.txt",
		})
		assert.NoError(err)
		assert.Len(resp.Body, 1)
		assert.Equal("mem://testbucket/file1.txt", resp.Body[0].URL)
	})

	t.Run("list empty prefix", func(t *testing.T) {
		assert := assert.New(t)

		resp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{
			URL: "mem://testbucket/nonexistent/",
		})
		assert.NoError(err)
		assert.Len(resp.Body, 0)
	})
}

////////////////////////////////////////////////////////////////////////////////
// MEM BACKEND - DELETEOBJECT TESTS

func Test_ManagerMem_DeleteObject(t *testing.T) {
	ctx := context.Background()

	mgr, err := New(ctx, WithBackend(ctx, "mem://testbucket"))
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("delete existing object", func(t *testing.T) {
		assert := assert.New(t)

		// Create object
		content := "delete me"
		_, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			URL:  "mem://testbucket/deleteme.txt",
			Body: strings.NewReader(content),
		})
		assert.NoError(err)

		// Delete it
		obj, err := mgr.DeleteObject(ctx, schema.DeleteObjectRequest{URL: "mem://testbucket/deleteme.txt"})
		assert.NoError(err)
		assert.Equal("mem://testbucket/deleteme.txt", obj.URL)
		assert.Equal(int64(len(content)), obj.Size)

		// Verify it's gone
		_, err = mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/deleteme.txt"})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})

	t.Run("delete non-existent object", func(t *testing.T) {
		assert := assert.New(t)

		_, err := mgr.DeleteObject(ctx, schema.DeleteObjectRequest{URL: "mem://testbucket/notfound.txt"})
		assert.Error(err)
		assert.Contains(err.Error(), "not found")
	})

	t.Run("delete nested object", func(t *testing.T) {
		assert := assert.New(t)

		// Create nested object
		_, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			URL:  "mem://testbucket/nested/deep/file.txt",
			Body: strings.NewReader("nested content"),
		})
		assert.NoError(err)

		// Delete it
		_, err = mgr.DeleteObject(ctx, schema.DeleteObjectRequest{URL: "mem://testbucket/nested/deep/file.txt"})
		assert.NoError(err)

		// Verify it's gone
		_, err = mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/nested/deep/file.txt"})
		assert.Error(err)
	})
}

////////////////////////////////////////////////////////////////////////////////
// MEM BACKEND - MULTIPLE BACKEND ROUTING TESTS

func Test_ManagerMem_RoutesToCorrectBackend(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	mgr, err := New(ctx,
		WithBackend(ctx, "mem://bucket1"),
		WithBackend(ctx, "mem://bucket2"),
	)
	require.NoError(t, err)
	defer mgr.Close()

	// Create in bucket1
	_, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		URL:  "mem://bucket1/test1.txt",
		Body: strings.NewReader("bucket1 content"),
	})
	assert.NoError(err)

	// Create in bucket2
	_, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		URL:  "mem://bucket2/test2.txt",
		Body: strings.NewReader("bucket2 content"),
	})
	assert.NoError(err)

	// Verify object in bucket1
	obj1, err := mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://bucket1/test1.txt"})
	assert.NoError(err)
	assert.Equal("mem://bucket1/test1.txt", obj1.URL)

	// Verify object in bucket2
	obj2, err := mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://bucket2/test2.txt"})
	assert.NoError(err)
	assert.Equal("mem://bucket2/test2.txt", obj2.URL)

	// Verify cross-bucket isolation
	_, err = mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://bucket1/test2.txt"})
	assert.Error(err)
	_, err = mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://bucket2/test1.txt"})
	assert.Error(err)
}

////////////////////////////////////////////////////////////////////////////////
// MEM BACKEND - FULL WORKFLOW TESTS

func Test_ManagerMem_FullWorkflow(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	mgr, err := New(ctx, WithBackend(ctx, "mem://testbucket"))
	require.NoError(t, err)
	defer mgr.Close()

	// 1. Create an object
	content := "full workflow test content"
	createdObj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
		URL:         "mem://testbucket/workflow/test.txt",
		Body:        strings.NewReader(content),
		ContentType: "text/plain",
		Meta:        schema.ObjectMeta{"step": "created"},
	})
	assert.NoError(err)
	assert.Equal("mem://testbucket/workflow/test.txt", createdObj.URL)

	// 2. Get object metadata
	gotObj, err := mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/workflow/test.txt"})
	assert.NoError(err)
	assert.Equal(int64(len(content)), gotObj.Size)
	assert.Equal("text/plain", gotObj.ContentType)

	// 3. Read object content
	reader, readObj, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{URL: "mem://testbucket/workflow/test.txt"})
	assert.NoError(err)
	data, _ := io.ReadAll(reader)
	reader.Close()
	assert.Equal(content, string(data))
	assert.Equal(int64(len(content)), readObj.Size)

	// 4. List objects
	listResp, err := mgr.ListObjects(ctx, schema.ListObjectsRequest{URL: "mem://testbucket/workflow/"})
	assert.NoError(err)
	assert.Len(listResp.Body, 1)

	// 5. Delete object
	deletedObj, err := mgr.DeleteObject(ctx, schema.DeleteObjectRequest{URL: "mem://testbucket/workflow/test.txt"})
	assert.NoError(err)
	assert.Equal("mem://testbucket/workflow/test.txt", deletedObj.URL)

	// 6. Verify object is gone
	_, err = mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/workflow/test.txt"})
	assert.Error(err)

	// 7. Verify list is empty
	listResp, err = mgr.ListObjects(ctx, schema.ListObjectsRequest{URL: "mem://testbucket/workflow/"})
	assert.NoError(err)
	assert.Len(listResp.Body, 0)
}

////////////////////////////////////////////////////////////////////////////////
// MEM BACKEND - EDGE CASES

func Test_ManagerMem_EdgeCases(t *testing.T) {
	ctx := context.Background()

	mgr, err := New(ctx, WithBackend(ctx, "mem://testbucket"))
	require.NoError(t, err)
	defer mgr.Close()

	t.Run("empty content", func(t *testing.T) {
		assert := assert.New(t)

		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			URL:  "mem://testbucket/empty.txt",
			Body: strings.NewReader(""),
		})
		assert.NoError(err)
		assert.Equal(int64(0), obj.Size)

		// Should be retrievable
		reader, _, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{URL: "mem://testbucket/empty.txt"})
		assert.NoError(err)
		data, _ := io.ReadAll(reader)
		reader.Close()
		assert.Equal("", string(data))
	})

	t.Run("binary content", func(t *testing.T) {
		assert := assert.New(t)

		binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			URL:         "mem://testbucket/binary.bin",
			Body:        bytes.NewReader(binaryData),
			ContentType: "application/octet-stream",
		})
		assert.NoError(err)
		assert.Equal(int64(len(binaryData)), obj.Size)

		// Should be retrievable
		reader, _, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{URL: "mem://testbucket/binary.bin"})
		assert.NoError(err)
		data, _ := io.ReadAll(reader)
		reader.Close()
		assert.Equal(binaryData, data)
	})

	t.Run("special characters in path", func(t *testing.T) {
		assert := assert.New(t)

		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			URL:  "mem://testbucket/path with spaces/file-name_v1.2.txt",
			Body: strings.NewReader("special chars"),
		})
		assert.NoError(err)
		assert.Contains(obj.URL, "path with spaces")

		// Should be retrievable
		_, err = mgr.GetObject(ctx, schema.GetObjectRequest{URL: "mem://testbucket/path with spaces/file-name_v1.2.txt"})
		assert.NoError(err)
	})

	t.Run("unicode content", func(t *testing.T) {
		assert := assert.New(t)

		unicodeContent := "Hello 世界! 🌍 Привет мир"
		obj, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
			URL:  "mem://testbucket/unicode.txt",
			Body: strings.NewReader(unicodeContent),
		})
		assert.NoError(err)
		assert.Equal(int64(len(unicodeContent)), obj.Size)

		// Should be retrievable
		reader, _, err := mgr.ReadObject(ctx, schema.ReadObjectRequest{URL: "mem://testbucket/unicode.txt"})
		assert.NoError(err)
		data, _ := io.ReadAll(reader)
		reader.Close()
		assert.Equal(unicodeContent, string(data))
	})
}
