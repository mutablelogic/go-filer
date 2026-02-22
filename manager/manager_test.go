package manager

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// MANAGER LIFECYCLE TESTS

func Test_Manager_New(t *testing.T) {
	assert := assert.New(t)

	t.Run("NewManager", func(t *testing.T) {
		mgr, err := New(context.TODO())
		assert.NoError(err)
		assert.NotNil(mgr)
	})
}

func Test_Manager_NewWithBackend(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	mgr, err := New(context.TODO(), WithFileBackend("test", tmpDir))
	assert.NoError(err)
	assert.NotNil(mgr)
	defer mgr.Close()
}

func Test_Manager_NewWithMultipleBackends(t *testing.T) {
	assert := assert.New(t)
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	mgr, err := New(context.TODO(),
		WithFileBackend("backend1", tmpDir1),
		WithFileBackend("backend2", tmpDir2),
	)
	assert.NoError(err)
	assert.NotNil(mgr)
	defer mgr.Close()
}

func Test_Manager_Close(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	mgr, err := New(context.TODO(), WithFileBackend("test", tmpDir))
	assert.NoError(err)

	err = mgr.Close()
	assert.NoError(err)
}

////////////////////////////////////////////////////////////////////////////////
// KEY ROUTING TESTS

func Test_Manager_Key(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	mgr, err := New(context.TODO(), WithFileBackend("test", tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	// Matching URL
	u, _ := url.Parse("file://test/somefile.txt")
	key := mgr.Key(u)
	assert.Equal("/somefile.txt", key)

	// Non-matching URL (wrong host)
	u2, _ := url.Parse("file://other/somefile.txt")
	key2 := mgr.Key(u2)
	assert.Equal("", key2)

	// Non-matching URL (wrong scheme)
	u3, _ := url.Parse("s3://test/somefile.txt")
	key3 := mgr.Key(u3)
	assert.Equal("", key3)
}

func Test_Manager_Key_MultipleBackends(t *testing.T) {
	assert := assert.New(t)
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	mgr, err := New(context.TODO(),
		WithFileBackend("files", tmpDir1),
		WithFileBackend("media", tmpDir2),
	)
	assert.NoError(err)
	defer mgr.Close()

	// First backend
	u1, _ := url.Parse("file://files/doc.txt")
	assert.Equal("/doc.txt", mgr.Key(u1))

	// Second backend
	u2, _ := url.Parse("file://media/video.mp4")
	assert.Equal("/video.mp4", mgr.Key(u2))

	// No match
	u3, _ := url.Parse("file://other/file.txt")
	assert.Equal("", mgr.Key(u3))
}

////////////////////////////////////////////////////////////////////////////////
// BACKEND ROUTING ERROR TESTS

func Test_Manager_NoBackendError(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	mgr, err := New(context.TODO(), WithFileBackend("test", tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()

	// GetObject with wrong backend
	_, err = mgr.GetObject(ctx, schema.GetObjectRequest{URL: "file://other/file.txt"})
	assert.Error(err)

	// ListObjects with wrong backend
	_, err = mgr.ListObjects(ctx, schema.ListObjectsRequest{URL: "file://other/"})
	assert.Error(err)

	// DeleteObject with wrong backend
	_, err = mgr.DeleteObject(ctx, schema.DeleteObjectRequest{URL: "file://other/file.txt"})
	assert.Error(err)

	// CreateObject with wrong backend
	_, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		URL:  "file://other/file.txt",
		Body: bytes.NewReader([]byte("content")),
	})
	assert.Error(err)

	// ReadObject with wrong backend
	_, _, err = mgr.ReadObject(ctx, schema.ReadObjectRequest{URL: "file://other/file.txt"})
	assert.Error(err)
}

////////////////////////////////////////////////////////////////////////////////
// OPERATION DELEGATION TESTS

func Test_Manager_CreateObject(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	mgr, err := New(context.TODO(), WithFileBackend("test", tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()
	content := []byte("hello from manager")
	req := schema.CreateObjectRequest{
		URL:  "file://test/created.txt",
		Body: bytes.NewReader(content),
	}

	obj, err := mgr.CreateObject(ctx, req)
	assert.NoError(err)
	assert.Equal("file://test/created.txt", obj.URL)
	assert.Equal(int64(len(content)), obj.Size)

	// Verify file exists
	data, err := os.ReadFile(filepath.Join(tmpDir, "created.txt"))
	assert.NoError(err)
	assert.Equal(content, data)
}

func Test_Manager_ReadObject(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	// Create a test file
	content := []byte("read me via manager")
	err := os.WriteFile(filepath.Join(tmpDir, "readable.txt"), content, 0644)
	assert.NoError(err)

	mgr, err := New(context.TODO(), WithFileBackend("test", tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()
	req := schema.ReadObjectRequest{URL: "file://test/readable.txt"}

	reader, obj, err := mgr.ReadObject(ctx, req)
	assert.NoError(err)
	defer reader.Close()

	assert.Equal("file://test/readable.txt", obj.URL)
	assert.Equal(int64(len(content)), obj.Size)

	data, err := io.ReadAll(reader)
	assert.NoError(err)
	assert.Equal(content, data)
}

func Test_Manager_GetObject(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	// Create a test file
	content := []byte("get my metadata")
	err := os.WriteFile(filepath.Join(tmpDir, "getme.txt"), content, 0644)
	assert.NoError(err)

	mgr, err := New(context.TODO(), WithFileBackend("test", tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()
	req := schema.GetObjectRequest{URL: "file://test/getme.txt"}

	obj, err := mgr.GetObject(ctx, req)
	assert.NoError(err)
	assert.Equal("file://test/getme.txt", obj.URL)
	assert.Equal(int64(len(content)), obj.Size)
}

func Test_Manager_ListObjects(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	// Create test files
	assert.NoError(os.WriteFile(filepath.Join(tmpDir, "file1.txt"), []byte("one"), 0644))
	assert.NoError(os.WriteFile(filepath.Join(tmpDir, "file2.txt"), []byte("two"), 0644))

	mgr, err := New(context.TODO(), WithFileBackend("test", tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()
	req := schema.ListObjectsRequest{URL: "file://test/"}

	resp, err := mgr.ListObjects(ctx, req)
	assert.NoError(err)
	assert.Equal("file://test/", resp.URL)
	assert.Len(resp.Body, 2)
}

func Test_Manager_DeleteObject(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	// Create a test file
	filePath := filepath.Join(tmpDir, "deleteme.txt")
	assert.NoError(os.WriteFile(filePath, []byte("delete me"), 0644))

	mgr, err := New(context.TODO(), WithFileBackend("test", tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()
	req := schema.DeleteObjectRequest{URL: "file://test/deleteme.txt"}

	obj, err := mgr.DeleteObject(ctx, req)
	assert.NoError(err)
	assert.Equal("file://test/deleteme.txt", obj.URL)

	// Verify file is deleted
	_, err = os.Stat(filePath)
	assert.True(os.IsNotExist(err))
}

////////////////////////////////////////////////////////////////////////////////
// MULTIPLE BACKEND ROUTING TESTS

func Test_Manager_RoutesToCorrectBackend(t *testing.T) {
	assert := assert.New(t)
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	mgr, err := New(context.TODO(),
		WithFileBackend("backend1", tmpDir1),
		WithFileBackend("backend2", tmpDir2),
	)
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()

	// Create in backend1
	_, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		URL:  "file://backend1/test1.txt",
		Body: bytes.NewReader([]byte("backend1 content")),
	})
	assert.NoError(err)

	// Create in backend2
	_, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		URL:  "file://backend2/test2.txt",
		Body: bytes.NewReader([]byte("backend2 content")),
	})
	assert.NoError(err)

	// Verify files are in correct directories
	_, err = os.Stat(filepath.Join(tmpDir1, "test1.txt"))
	assert.NoError(err)
	_, err = os.Stat(filepath.Join(tmpDir2, "test2.txt"))
	assert.NoError(err)

	// Verify files are NOT in wrong directories
	_, err = os.Stat(filepath.Join(tmpDir1, "test2.txt"))
	assert.True(os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(tmpDir2, "test1.txt"))
	assert.True(os.IsNotExist(err))
}
