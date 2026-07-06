package manager_test

import (
	"bytes"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	test "github.com/mutablelogic/go-filer/filer/test"
	types "github.com/mutablelogic/go-server/pkg/types"
	require "github.com/stretchr/testify/require"
)

func TestObjectCreateGetList(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	volume := createMountedFileVolume(t, "test_object_create_get_list")

	body := []byte("hello object")
	created, isNew, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
		ObjectKey: schema.ObjectKey{Volume: volume, Path: "alpha/one.txt"},
		Body:      bytes.NewReader(body),
	}, schema.ObjectAttr{})
	require.NoError(err)
	require.True(isNew)
	require.NotNil(created)
	require.Equal(volume, created.Volume)
	require.Equal("alpha/one.txt", created.Path)
	require.Equal(int64(len(body)), types.Value(created.Size))
	require.False(created.ModTime.IsZero())
	require.NotEmpty(created.ContentType)

	got, err := mgr.GetObject(ctx, schema.ObjectKey{Volume: volume, Path: "alpha/one.txt"})
	require.NoError(err)
	require.NotNil(got)
	require.Equal(created.Path, got.Path)
	require.Equal(types.Value(created.Size), types.Value(got.Size))
	require.Equal(created.ContentType, got.ContentType)

	_, isNew, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		ObjectKey: schema.ObjectKey{Volume: volume, Path: "beta/two.txt"},
		Body:      bytes.NewReader([]byte("second object")),
	}, schema.ObjectAttr{})
	require.NoError(err)
	require.True(isNew)

	objects, err := mgr.ListObjects(ctx, schema.ObjectListRequest{
		Volume: volume,
		ObjectListFilters: schema.ObjectListFilters{
			Recursive: true,
		},
	})
	require.NoError(err)
	require.NotNil(objects)
	require.ElementsMatch([]string{"alpha/one.txt", "beta/two.txt"}, objectPaths(objects.Body))

	dirs, err := mgr.ListObjects(ctx, schema.ObjectListRequest{
		Volume: volume,
		ObjectListFilters: schema.ObjectListFilters{
			Type: types.Ptr(schema.ContentTypeDirectory),
		},
	})
	require.NoError(err)
	require.NotNil(dirs)
	require.ElementsMatch([]string{"alpha", "beta"}, objectPaths(dirs.Body))
}

func TestObjectListIndexedNonRecursiveRoot(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	dir := t.TempDir()
	require.NoError(os.WriteFile(filepath.Join(dir, "root.txt"), []byte("root"), 0644))
	require.NoError(os.MkdirAll(filepath.Join(dir, "nested", "deeper"), 0755))
	require.NoError(os.WriteFile(filepath.Join(dir, "nested", "child.txt"), []byte("child"), 0644))
	require.NoError(os.WriteFile(filepath.Join(dir, "nested", "deeper", "grandchild.txt"), []byte("grandchild"), 0644))

	volume := "test_object_list_indexed_non_recursive_root"
	_, err := mgr.CreateVolume(ctx, &url.URL{Scheme: "file", Host: volume, Path: dir}, schema.VolumeMeta{})
	require.NoError(err)
	_, err = mgr.UpdateVolume(ctx, volume, schema.VolumeMeta{IndexDelta: types.Ptr(time.Minute)})
	require.NoError(err)
	require.Eventually(func() bool {
		return mgr.ReindexVolume(ctx, volume, schema.ObjectListFilters{Recursive: true}, false, nil) == nil
	}, 5*time.Second, 20*time.Millisecond, "volume did not mount in time")

	root, err := mgr.ListObjects(ctx, schema.ObjectListRequest{Volume: volume})
	require.NoError(err)
	require.NotNil(root)
	require.ElementsMatch([]string{"root.txt"}, objectPaths(root.Body))

	recursive, err := mgr.ListObjects(ctx, schema.ObjectListRequest{
		Volume: volume,
		ObjectListFilters: schema.ObjectListFilters{
			Recursive: true,
		},
	})
	require.NoError(err)
	require.NotNil(recursive)
	require.ElementsMatch([]string{"root.txt", "nested/child.txt", "nested/deeper/grandchild.txt"}, objectPaths(recursive.Body))
}

func TestObjectGetObjectDeletesStaleIndex(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "stale.txt")
	require.NoError(os.WriteFile(path, []byte("stale"), 0644))

	volume := "test_object_get_object_deletes_stale_index"
	_, err := mgr.CreateVolume(ctx, &url.URL{Scheme: "file", Host: volume, Path: dir}, schema.VolumeMeta{})
	require.NoError(err)
	_, err = mgr.UpdateVolume(ctx, volume, schema.VolumeMeta{IndexDelta: types.Ptr(time.Minute)})
	require.NoError(err)
	require.Eventually(func() bool {
		return mgr.ReindexVolume(ctx, volume, schema.ObjectListFilters{Recursive: true}, false, nil) == nil
	}, 5*time.Second, 20*time.Millisecond, "volume did not mount in time")

	indexed, err := mgr.ListObjects(ctx, schema.ObjectListRequest{
		Volume: volume,
		ObjectListFilters: schema.ObjectListFilters{
			Recursive: true,
		},
	})
	require.NoError(err)
	require.ElementsMatch([]string{"stale.txt"}, objectPaths(indexed.Body))

	require.NoError(os.Remove(path))
	_, err = mgr.GetObject(ctx, schema.ObjectKey{Volume: volume, Path: "stale.txt"})
	require.ErrorIs(err, gofiler.ErrNotFound)

	indexed, err = mgr.ListObjects(ctx, schema.ObjectListRequest{
		Volume: volume,
		ObjectListFilters: schema.ObjectListFilters{
			Recursive: true,
		},
	})
	require.NoError(err)
	require.Empty(indexed.Body)
}

func TestObjectCreatePreconditions(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	volume := createMountedFileVolume(t, "test_object_create_preconditions")

	created, isNew, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
		ObjectKey: schema.ObjectKey{Volume: volume, Path: "same.txt"},
		Body:      bytes.NewReader([]byte("original")),
		ModTime:   time.Unix(1000, 0).UTC(),
	}, schema.ObjectAttr{})
	require.NoError(err)
	require.True(isNew)
	require.NotNil(created)
	require.True(created.ModTime.Equal(time.Unix(1000, 0).UTC()))

	_, _, err = mgr.CreateObject(ctx, schema.CreateObjectRequest{
		ObjectKey:   schema.ObjectKey{Volume: volume, Path: "same.txt"},
		Body:        bytes.NewReader([]byte("replacement")),
		IfNotExists: true,
	}, schema.ObjectAttr{})
	require.ErrorIs(err, gofiler.ErrConflict)

	current, isNew, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
		ObjectKey: schema.ObjectKey{Volume: volume, Path: "same.txt"},
		Body:      bytes.NewReader([]byte("replacement")),
	}, schema.ObjectAttr{
		Size:    created.Size,
		ModTime: created.ModTime,
	})
	require.ErrorIs(err, gofiler.ErrNotModified)
	require.False(isNew)
	require.NotNil(current)
	require.Equal(created.Path, current.Path)
	require.Equal(types.Value(created.Size), types.Value(current.Size))
}

func TestObjectReadObject(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	volume := createMountedFileVolume(t, "test_object_read_object")
	body := []byte("read object body")
	created, _, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
		ObjectKey: schema.ObjectKey{Volume: volume, Path: "read.txt"},
		Body:      bytes.NewReader(body),
	}, schema.ObjectAttr{})
	require.NoError(err)
	require.NotNil(created)

	reader, object, err := mgr.ReadObject(ctx, schema.ObjectKey{Volume: volume, Path: "read.txt"}, schema.ObjectAttr{})
	require.NoError(err)
	require.NotNil(reader)
	defer reader.Close()
	require.NotNil(object)
	require.Equal(created.Path, object.Path)
	require.Equal(types.Value(created.Size), types.Value(object.Size))

	data, err := io.ReadAll(reader)
	require.NoError(err)
	require.Equal(body, data)
}

func TestObjectReadObjectNotModified(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	volume := createMountedFileVolume(t, "test_object_read_object_not_modified")
	body := []byte("read object not modified")
	created, _, err := mgr.CreateObject(ctx, schema.CreateObjectRequest{
		ObjectKey: schema.ObjectKey{Volume: volume, Path: "cached.txt"},
		Body:      bytes.NewReader(body),
	}, schema.ObjectAttr{})
	require.NoError(err)
	require.NotNil(created)

	reader, object, err := mgr.ReadObject(ctx, schema.ObjectKey{Volume: volume, Path: "cached.txt"}, schema.ObjectAttr{
		Size: created.Size,
	})
	require.ErrorIs(err, gofiler.ErrNotModified)
	require.Nil(reader)
	require.NotNil(object)
	require.Equal(created.Path, object.Path)
	require.Equal(types.Value(created.Size), types.Value(object.Size))
}

func createMountedFileVolume(t *testing.T, name string) string {
	t.Helper()
	require := require.New(t)
	mgr, ctx := test.Begin(t)

	_, err := mgr.CreateVolume(ctx, &url.URL{
		Scheme: "file",
		Host:   name,
		Path:   t.TempDir(),
	}, schema.VolumeMeta{})
	require.NoError(err)

	require.Eventually(func() bool {
		_, err := mgr.ListObjects(ctx, schema.ObjectListRequest{Volume: name})
		return err == nil
	}, 5*time.Second, 20*time.Millisecond, "volume did not mount in time")

	return name
}

func objectPaths(objects []*schema.Object) []string {
	paths := make([]string, 0, len(objects))
	for _, object := range objects {
		paths = append(paths, object.Path)
	}
	return paths
}
