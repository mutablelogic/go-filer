package manager_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	test "github.com/mutablelogic/go-filer/filer/test"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
	require "github.com/stretchr/testify/require"
)

///////////////////////////////////////////////////////////////////////////////
// TESTS

func TestVolumeLifecycle(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	volURL := &url.URL{Scheme: "file", Host: "test_volume", Path: t.TempDir()}

	// Create a volume backed by a temporary directory
	created, err := mgr.CreateVolume(ctx, volURL, schema.VolumeMeta{})
	require.NoError(err)
	require.NotNil(created)
	require.Equal("test_volume", created.Name)
	require.True(types.Value(created.Enabled))

	// List volumes and check the new volume is present
	list, err := mgr.ListVolumes(ctx, schema.VolumeListRequest{})
	require.NoError(err)
	require.NotNil(list)

	var found *schema.Volume
	for _, volume := range list.Body {
		if volume.Name == "test_volume" {
			found = volume
		}
	}
	require.NotNil(found)
	require.True(types.Value(found.Enabled))

	// Update the volume's index delta
	updated, err := mgr.UpdateVolume(ctx, "test_volume", schema.VolumeMeta{
		IndexDelta: types.Ptr(time.Minute),
	})
	require.NoError(err)
	require.NotNil(updated)
	require.Equal(time.Minute, types.Value(updated.IndexDelta))

	// Disable the volume
	disabled, err := mgr.UpdateVolume(ctx, "test_volume", schema.VolumeMeta{
		Enabled: types.Ptr(false),
	})
	require.NoError(err)
	require.NotNil(disabled)
	require.False(types.Value(disabled.Enabled))

	// Delete the volume, now that it is disabled
	deleted, err := mgr.DeleteVolume(ctx, "test_volume")
	require.NoError(err)
	require.NotNil(deleted)
	require.Equal("test_volume", deleted.Name)

	// The volume should no longer exist
	_, err = mgr.GetVolume(ctx, "test_volume")
	require.Error(err)
	require.ErrorIs(err, pg.ErrNotFound)
}

func TestVolumeDeleteWhileEnabled(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	volURL := &url.URL{Scheme: "file", Host: "test_volume_enabled", Path: t.TempDir()}

	_, err := mgr.CreateVolume(ctx, volURL, schema.VolumeMeta{})
	require.NoError(err)

	// Deleting an enabled (mounted) volume should fail
	_, err = mgr.DeleteVolume(ctx, "test_volume_enabled")
	require.Error(err)
}

func TestVolumeReindexObject(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	dir := t.TempDir()
	require.NoError(os.WriteFile(filepath.Join(dir, "object.txt"), []byte("hello"), 0644))
	require.NoError(os.Mkdir(filepath.Join(dir, "subdir"), 0755))

	volURL := &url.URL{Scheme: "file", Host: "test_volume_reindex", Path: dir}
	_, err := mgr.CreateVolume(ctx, volURL, schema.VolumeMeta{})
	require.NoError(err)

	_, err = mgr.UpdateVolume(ctx, "test_volume_reindex", schema.VolumeMeta{
		IndexDelta: types.Ptr(time.Minute),
	})
	require.NoError(err)

	// Volume mounting happens asynchronously in response to the change
	// notification, so wait for it before reindexing.
	require.Eventually(func() bool {
		return mgr.ReindexVolume(ctx, "test_volume_reindex", schema.ObjectListFilters{
			Path: types.Ptr("object.txt"),
		}, false) == nil
	}, 5*time.Second, 20*time.Millisecond, "volume did not mount in time")

	// Reindexing a directory should still work as before
	err = mgr.ReindexVolume(ctx, "test_volume_reindex", schema.ObjectListFilters{
		Path: types.Ptr("subdir"),
	}, false)
	require.NoError(err)

	// Reindexing a path that doesn't exist should fail
	err = mgr.ReindexVolume(ctx, "test_volume_reindex", schema.ObjectListFilters{
		Path: types.Ptr("does-not-exist"),
	}, false)
	require.Error(err)
}
