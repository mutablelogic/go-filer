package manager_test

import (
	"context"
	"fmt"
	"testing"

	// Packages
	manager "github.com/mutablelogic/go-filer/credential/manager"
	schema "github.com/mutablelogic/go-filer/credential/schema"
	test "github.com/mutablelogic/go-filer/credential/test"
	pg "github.com/mutablelogic/go-pg"
	require "github.com/stretchr/testify/require"
)

const listSeedCount = 5

// seedCredentials creates N credentials with keys list_N_0 … list_N_4 and
// returns a cleanup function that deletes them.
func seedCredentials(t *testing.T, n int) {
	t.Helper()
	mgr, ctx := test.Begin(t)

	for i := range n {
		key := fmt.Sprintf("list_%d_%d", n, i)
		_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
			CredentialKey: schema.CredentialKey{Key: key},
			Credentials:   fmt.Sprintf("secret-%d", i),
		})
		if err != nil {
			t.Fatalf("seed credential %s: %v", key, err)
		}
	}

	t.Cleanup(func() {
		for i := range n {
			key := fmt.Sprintf("list_%d_%d", n, i)
			mgr.DeleteCredential(ctx, schema.CredentialKey{Key: key}) //nolint:errcheck
		}
	})
}

func TestListAll(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	seedCredentials(t, listSeedCount)

	result, err := mgr.ListCredentials(ctx, schema.CredentialListRequest{})
	require.NoError(err)
	require.NotNil(result)
	require.GreaterOrEqual(result.Count, uint64(listSeedCount))
	require.GreaterOrEqual(uint64(len(result.Body)), uint64(listSeedCount))
}

func TestListWithLimit(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	seedCredentials(t, listSeedCount)

	limit := uint64(2)
	result, err := mgr.ListCredentials(ctx, schema.CredentialListRequest{
		OffsetLimit: pg.OffsetLimit{Limit: &limit},
	})
	require.NoError(err)
	require.NotNil(result)
	require.GreaterOrEqual(result.Count, uint64(listSeedCount))
	require.Len(result.Body, int(limit))
}

func TestListWithOffset(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	seedCredentials(t, listSeedCount)

	// Fetch all to establish the full ordered set.
	all, err := mgr.ListCredentials(ctx, schema.CredentialListRequest{})
	require.NoError(err)

	// Fetch with offset=2, limit=2 and compare to the equivalent slice of all.
	limit := uint64(2)
	result, err := mgr.ListCredentials(ctx, schema.CredentialListRequest{
		OffsetLimit: pg.OffsetLimit{Offset: 2, Limit: &limit},
	})
	require.NoError(err)
	require.NotNil(result)
	require.Equal(result.Count, all.Count)
	require.Len(result.Body, int(limit))
	require.Equal(all.Body[2].Key, result.Body[0].Key)
	require.Equal(all.Body[3].Key, result.Body[1].Key)
}

func TestListPastEnd(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	seedCredentials(t, listSeedCount)

	all, err := mgr.ListCredentials(ctx, schema.CredentialListRequest{})
	require.NoError(err)

	// Offset beyond the total count should return an empty body.
	result, err := mgr.ListCredentials(ctx, schema.CredentialListRequest{
		OffsetLimit: pg.OffsetLimit{Offset: all.Count + 10},
	})
	require.NoError(err)
	require.NotNil(result)
	require.Empty(result.Body)
}

func TestListRotateNeedsRotation(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	// Seed 2 credentials at PV=1 (needs rotation relative to PV=2).
	pv1Mgr, err := manager.New(context.Background(), mgr.PoolConn,
		manager.WithPassphrase(1, "old-passphrase"),
	)
	require.NoError(err)
	for i := range 2 {
		_, err := pv1Mgr.CreateCredential(ctx, schema.CredentialCreate{
			CredentialKey: schema.CredentialKey{Key: fmt.Sprintf("rotate_filter_old_%d", i)},
			Credentials:   fmt.Sprintf("old-secret-%d", i),
		})
		require.NoError(err)
	}
	t.Cleanup(func() {
		for i := range 2 {
			mgr.DeleteCredential(ctx, schema.CredentialKey{Key: fmt.Sprintf("rotate_filter_old_%d", i)}) //nolint:errcheck
		}
	})

	// Seed 2 credentials at PV=2 (already current).
	pv2Mgr, err := manager.New(context.Background(), mgr.PoolConn,
		manager.WithPassphrase(1, "old-passphrase"),
		manager.WithPassphrase(2, "new-passphrase"),
	)
	require.NoError(err)
	for i := range 2 {
		_, err := pv2Mgr.CreateCredential(ctx, schema.CredentialCreate{
			CredentialKey: schema.CredentialKey{Key: fmt.Sprintf("rotate_filter_new_%d", i)},
			Credentials:   fmt.Sprintf("new-secret-%d", i),
		})
		require.NoError(err)
	}
	t.Cleanup(func() {
		for i := range 2 {
			mgr.DeleteCredential(ctx, schema.CredentialKey{Key: fmt.Sprintf("rotate_filter_new_%d", i)}) //nolint:errcheck
		}
	})

	// Rotate=true with pv2Mgr (latestpv=2): should include the PV=1 credentials.
	rotateTrue := true
	result, err := pv2Mgr.ListCredentials(ctx, schema.CredentialListRequest{Rotate: &rotateTrue})
	require.NoError(err)
	require.NotNil(result)
	require.GreaterOrEqual(result.Count, uint64(2))
	keys := make(map[string]bool, len(result.Body))
	for _, c := range result.Body {
		keys[c.Key] = true
	}
	require.True(keys["rotate_filter_old_0"])
	require.True(keys["rotate_filter_old_1"])
	require.False(keys["rotate_filter_new_0"])
	require.False(keys["rotate_filter_new_1"])
}

func TestListRotateCurrent(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	// Seed 2 credentials at PV=1.
	pv1Mgr, err := manager.New(context.Background(), mgr.PoolConn,
		manager.WithPassphrase(1, "old-passphrase"),
	)
	require.NoError(err)
	for i := range 2 {
		_, err := pv1Mgr.CreateCredential(ctx, schema.CredentialCreate{
			CredentialKey: schema.CredentialKey{Key: fmt.Sprintf("rotate_current_old_%d", i)},
			Credentials:   fmt.Sprintf("old-secret-%d", i),
		})
		require.NoError(err)
	}
	t.Cleanup(func() {
		for i := range 2 {
			mgr.DeleteCredential(ctx, schema.CredentialKey{Key: fmt.Sprintf("rotate_current_old_%d", i)}) //nolint:errcheck
		}
	})

	// Seed 2 credentials at PV=2.
	pv2Mgr, err := manager.New(context.Background(), mgr.PoolConn,
		manager.WithPassphrase(1, "old-passphrase"),
		manager.WithPassphrase(2, "new-passphrase"),
	)
	require.NoError(err)
	for i := range 2 {
		_, err := pv2Mgr.CreateCredential(ctx, schema.CredentialCreate{
			CredentialKey: schema.CredentialKey{Key: fmt.Sprintf("rotate_current_new_%d", i)},
			Credentials:   fmt.Sprintf("new-secret-%d", i),
		})
		require.NoError(err)
	}
	t.Cleanup(func() {
		for i := range 2 {
			mgr.DeleteCredential(ctx, schema.CredentialKey{Key: fmt.Sprintf("rotate_current_new_%d", i)}) //nolint:errcheck
		}
	})

	// Rotate=false with pv2Mgr (latestpv=2): should include only the PV=2 credentials.
	rotateFalse := false
	result, err := pv2Mgr.ListCredentials(ctx, schema.CredentialListRequest{Rotate: &rotateFalse})
	require.NoError(err)
	require.NotNil(result)
	require.GreaterOrEqual(result.Count, uint64(2))
	keys := make(map[string]bool, len(result.Body))
	for _, c := range result.Body {
		keys[c.Key] = true
	}
	require.True(keys["rotate_current_new_0"])
	require.True(keys["rotate_current_new_1"])
	require.False(keys["rotate_current_old_0"])
	require.False(keys["rotate_current_old_1"])
}
