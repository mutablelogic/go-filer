package manager_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	// Packages
	manager "github.com/mutablelogic/go-filer/credential/manager"
	schema "github.com/mutablelogic/go-filer/credential/schema"
	test "github.com/mutablelogic/go-filer/credential/test"
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	require "github.com/stretchr/testify/require"
)

func TestRotateNotModified(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	// The shared manager has a single passphrase (PV=1), so the credential is
	// already at the latest version and rotation should be a no-op.
	_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "rotate_noop"},
		Credentials:   "unchanged-secret",
	})
	require.NoError(err)

	_, err = mgr.RotateCredential(ctx, schema.CredentialKey{Key: "rotate_noop"})
	require.Error(err)
	require.True(errors.Is(err, httpresponse.Err(http.StatusNotModified)))
}

func TestRotateNonExistent(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.RotateCredential(ctx, schema.CredentialKey{Key: "rotate_missing"})
	require.Error(err)
	require.True(errors.Is(err, pg.ErrNotFound))
}

func TestRotateCredential(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	// Seed a credential at PV=1 using a single-passphrase manager.
	oldMgr, err := manager.New(context.Background(), mgr.PoolConn,
		manager.WithPassphrase(1, "old-passphrase"),
	)
	require.NoError(err)

	_, err = oldMgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "rotate_upgrade"},
		Credentials:   "rotate-me",
	})
	require.NoError(err)

	// Create a two-passphrase manager (PV=1 + PV=2) and rotate.
	rotateMgr, err := manager.New(context.Background(), mgr.PoolConn,
		manager.WithPassphrase(1, "old-passphrase"),
		manager.WithPassphrase(2, "new-passphrase"),
	)
	require.NoError(err)

	cred, err := rotateMgr.RotateCredential(ctx, schema.CredentialKey{Key: "rotate_upgrade"})
	require.NoError(err)
	require.NotNil(cred)
	require.Equal("rotate_upgrade", cred.Key)

	// Credential should now be accessible with the new passphrase (PV=2).
	got, err := rotateMgr.GetCredential(ctx, schema.CredentialKey{Key: "rotate_upgrade"}, "new-passphrase")
	require.NoError(err)
	var value string
	require.NoError(json.Unmarshal(got, &value))
	require.Equal("rotate-me", value)

	// Getting with the old passphrase (PV=1) should fail — the row now stores PV=2.
	_, err = rotateMgr.GetCredential(ctx, schema.CredentialKey{Key: "rotate_upgrade"}, "old-passphrase")
	require.Error(err)
}
