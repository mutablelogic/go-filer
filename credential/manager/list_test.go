package manager_test

import (
	"fmt"
	"testing"

	// Packages
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
