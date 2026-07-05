package manager_test

import (
	"errors"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/credential/schema"
	test "github.com/mutablelogic/go-filer/credential/test"
	pg "github.com/mutablelogic/go-pg"
	require "github.com/stretchr/testify/require"
)

func TestDeleteCredential(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "del_existing"},
		Credentials:   "token-to-delete",
	})
	require.NoError(err)

	deleted, err := mgr.DeleteCredential(ctx, schema.CredentialKey{Key: "del_existing"})
	require.NoError(err)
	require.NotNil(deleted)
	require.Equal("del_existing", deleted.Key)
}

func TestDeleteCredentialGone(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "del_gone"},
		Credentials:   "token",
	})
	require.NoError(err)

	_, err = mgr.DeleteCredential(ctx, schema.CredentialKey{Key: "del_gone"})
	require.NoError(err)

	// Second delete must report not found.
	_, err = mgr.DeleteCredential(ctx, schema.CredentialKey{Key: "del_gone"})
	require.Error(err)
	require.True(errors.Is(err, pg.ErrNotFound))
}

func TestDeleteNonExistentCredential(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.DeleteCredential(ctx, schema.CredentialKey{Key: "del_missing"})
	require.Error(err)
	require.True(errors.Is(err, pg.ErrNotFound))
}
