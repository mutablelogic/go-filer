package manager

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
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

	mgr, err := New(context.TODO(), WithBackend(context.TODO(), "file://test"+tmpDir))
	assert.NoError(err)
	assert.NotNil(mgr)
	defer mgr.Close()
}

func Test_Manager_NewWithMultipleBackends(t *testing.T) {
	assert := assert.New(t)
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	mgr, err := New(context.TODO(),
		WithBackend(context.TODO(), "file://backend1"+tmpDir1),
		WithBackend(context.TODO(), "file://backend2"+tmpDir2),
	)
	assert.NoError(err)
	assert.NotNil(mgr)
	defer mgr.Close()
}

func Test_Manager_DuplicateBackendName(t *testing.T) {
	assert := assert.New(t)
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	_, err := New(context.TODO(),
		WithBackend(context.TODO(), "file://samename"+tmpDir1),
		WithBackend(context.TODO(), "file://samename"+tmpDir2),
	)
	assert.Error(err)
}

func Test_Manager_Close(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	mgr, err := New(context.TODO(), WithBackend(context.TODO(), "file://test"+tmpDir))
	assert.NoError(err)

	err = mgr.Close()
	assert.NoError(err)
}

////////////////////////////////////////////////////////////////////////////////
// KEY ROUTING TESTS

func Test_Manager_Key(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	mgr, err := New(context.TODO(), WithBackend(context.TODO(), "file://test"+tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	// Matching backend and path
	assert.Equal("/somefile.txt", mgr.Key("test", "/somefile.txt"))

	// No backend with this name
	assert.Equal("", mgr.Key("other", "/somefile.txt"))
}

func Test_Manager_Key_MultipleBackends(t *testing.T) {
	assert := assert.New(t)
	tmpDir1 := t.TempDir()
	tmpDir2 := t.TempDir()

	mgr, err := New(context.TODO(),
		WithBackend(context.TODO(), "file://files"+tmpDir1),
		WithBackend(context.TODO(), "file://media"+tmpDir2),
	)
	assert.NoError(err)
	defer mgr.Close()

	// First backend
	assert.Equal("/doc.txt", mgr.Key("files", "/doc.txt"))

	// Second backend
	assert.Equal("/video.mp4", mgr.Key("media", "/video.mp4"))

	// No backend with this name
	assert.Equal("", mgr.Key("other", "/file.txt"))
}

////////////////////////////////////////////////////////////////////////////////
// BACKEND ROUTING ERROR TESTS

func Test_Manager_NoBackendError(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	mgr, err := New(context.TODO(), WithBackend(context.TODO(), "file://test"+tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()

	// GetObject with wrong backend
	_, err = mgr.GetObject(ctx, "other", schema.GetObjectRequest{Path: "/file.txt"})
	assert.Error(err)

	// ListObjects with wrong backend
	_, err = mgr.ListObjects(ctx, "other", schema.ListObjectsRequest{Path: "/"})
	assert.Error(err)

	// DeleteObject with wrong backend
	_, err = mgr.DeleteObject(ctx, "other", schema.DeleteObjectRequest{Path: "/file.txt"})
	assert.Error(err)

	// CreateObject with wrong backend
	_, err = mgr.CreateObject(ctx, "other", schema.CreateObjectRequest{
		Path: "/file.txt",
		Body: bytes.NewReader([]byte("content")),
	})
	assert.Error(err)

	// ReadObject with wrong backend
	_, _, err = mgr.ReadObject(ctx, "other", schema.ReadObjectRequest{Path: "/file.txt"})
	assert.Error(err)
}

////////////////////////////////////////////////////////////////////////////////
// OPERATION DELEGATION TESTS

func Test_Manager_CreateObject(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	mgr, err := New(context.TODO(), WithBackend(context.TODO(), "file://test"+tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()
	content := []byte("hello from manager")
	req := schema.CreateObjectRequest{
		Path: "/created.txt",
		Body: bytes.NewReader(content),
	}

	obj, err := mgr.CreateObject(ctx, "test", req)
	assert.NoError(err)
	assert.Equal("test", obj.Name)
	assert.Equal("/created.txt", obj.Path)
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

	mgr, err := New(context.TODO(), WithBackend(context.TODO(), "file://test"+tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()
	req := schema.ReadObjectRequest{Path: "/readable.txt"}

	reader, obj, err := mgr.ReadObject(ctx, "test", req)
	assert.NoError(err)
	defer reader.Close()

	assert.Equal("test", obj.Name)
	assert.Equal("/readable.txt", obj.Path)
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

	mgr, err := New(context.TODO(), WithBackend(context.TODO(), "file://test"+tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()
	req := schema.GetObjectRequest{Path: "/getme.txt"}

	obj, err := mgr.GetObject(ctx, "test", req)
	assert.NoError(err)
	assert.Equal("test", obj.Name)
	assert.Equal("/getme.txt", obj.Path)
	assert.Equal(int64(len(content)), obj.Size)
}

func Test_Manager_ListObjects(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	// Create test files
	assert.NoError(os.WriteFile(filepath.Join(tmpDir, "file1.txt"), []byte("one"), 0644))
	assert.NoError(os.WriteFile(filepath.Join(tmpDir, "file2.txt"), []byte("two"), 0644))

	mgr, err := New(context.TODO(), WithBackend(context.TODO(), "file://test"+tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()
	req := schema.ListObjectsRequest{Path: "/", Limit: schema.MaxListLimit}

	resp, err := mgr.ListObjects(ctx, "test", req)
	assert.NoError(err)
	assert.Equal("test", resp.Name)
	assert.Len(resp.Body, 2)
}

func Test_Manager_DeleteObject(t *testing.T) {
	assert := assert.New(t)
	tmpDir := t.TempDir()

	// Create a test file
	filePath := filepath.Join(tmpDir, "deleteme.txt")
	assert.NoError(os.WriteFile(filePath, []byte("delete me"), 0644))

	mgr, err := New(context.TODO(), WithBackend(context.TODO(), "file://test"+tmpDir))
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()
	req := schema.DeleteObjectRequest{Path: "/deleteme.txt"}

	obj, err := mgr.DeleteObject(ctx, "test", req)
	assert.NoError(err)
	assert.Equal("test", obj.Name)
	assert.Equal("/deleteme.txt", obj.Path)

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
		WithBackend(context.TODO(), "file://backend1"+tmpDir1),
		WithBackend(context.TODO(), "file://backend2"+tmpDir2),
	)
	assert.NoError(err)
	defer mgr.Close()

	ctx := context.Background()

	// Create in backend1
	_, err = mgr.CreateObject(ctx, "backend1", schema.CreateObjectRequest{
		Path: "/test1.txt",
		Body: bytes.NewReader([]byte("backend1 content")),
	})
	assert.NoError(err)

	// Create in backend2
	_, err = mgr.CreateObject(ctx, "backend2", schema.CreateObjectRequest{
		Path: "/test2.txt",
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
