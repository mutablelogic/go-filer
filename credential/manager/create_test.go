package manager_test

import (
	"encoding/json"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/credential/schema"
	test "github.com/mutablelogic/go-filer/credential/test"
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	require "github.com/stretchr/testify/require"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type s3Credentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Region          string `json:"region"`
}

///////////////////////////////////////////////////////////////////////////////
// TESTS

func TestCreateStringCredential(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	cred, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "test_string"},
		Credentials:   "my-secret-token",
	})
	require.NoError(err)
	require.NotNil(cred)
	require.Equal("test_string", cred.Key)

	got, err := mgr.GetCredential(ctx, schema.CredentialKey{Key: "test_string"}, "test-passphrase")
	require.NoError(err)

	var value string
	require.NoError(json.Unmarshal(got, &value))
	require.Equal("my-secret-token", value)
}

func TestCreateStructCredential(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	input := s3Credentials{
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Region:          "us-east-1",
	}

	cred, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "test_s3"},
		Credentials:   input,
	})
	require.NoError(err)
	require.NotNil(cred)
	require.Equal("test_s3", cred.Key)

	got, err := mgr.GetCredential(ctx, schema.CredentialKey{Key: "test_s3"}, "test-passphrase")
	require.NoError(err)

	var output s3Credentials
	require.NoError(json.Unmarshal(got, &output))
	require.Equal(input, output)
}

func TestCreateInvalidKey(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "bad-key!"},
		Credentials:   "secret",
	})
	require.Error(err)
	require.ErrorIs(err, httpresponse.ErrBadRequest)
}

func TestCreateEmptyCredentials(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "empty_cred"},
		Credentials:   "",
	})
	require.Error(err)
	require.ErrorIs(err, httpresponse.ErrBadRequest)
}

func TestCreateUpsert(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "upsert_key"},
		Credentials:   "original-secret",
	})
	require.NoError(err)

	// Second create with the same key should overwrite.
	_, err = mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "upsert_key"},
		Credentials:   "updated-secret",
	})
	require.NoError(err)

	got, err := mgr.GetCredential(ctx, schema.CredentialKey{Key: "upsert_key"}, "test-passphrase")
	require.NoError(err)
	var value string
	require.NoError(json.Unmarshal(got, &value))
	require.Equal("updated-secret", value)
}

func TestCreateGetNonExistent(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.GetCredential(ctx, schema.CredentialKey{Key: "never_created"}, "test-passphrase")
	require.Error(err)
	require.ErrorIs(err, pg.ErrNotFound)
}

func TestGetWithoutPassphraseNonExistent(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.GetCredentialWithoutPassphrase(ctx, schema.CredentialKey{Key: "also_never_created"})
	require.Error(err)
	require.ErrorIs(err, pg.ErrNotFound)
}
