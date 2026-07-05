package manager_test

import (
	"encoding/json"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/credential/schema"
	test "github.com/mutablelogic/go-filer/credential/test"
	require "github.com/stretchr/testify/require"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type googleCredentials struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RefreshToken string `json:"refresh_token"`
}

///////////////////////////////////////////////////////////////////////////////
// TESTS - GetCredential (requires passphrase)

func TestGetCredential(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "get_string"},
		Credentials:   "my-api-key",
	})
	require.NoError(err)

	got, err := mgr.GetCredential(ctx, schema.CredentialKey{Key: "get_string"}, "test-passphrase")
	require.NoError(err)

	var value string
	require.NoError(json.Unmarshal(got, &value))
	require.Equal("my-api-key", value)
}

func TestGetCredentialWrongPassphrase(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "get_wrong_pass"},
		Credentials:   "secret",
	})
	require.NoError(err)

	_, err = mgr.GetCredential(ctx, schema.CredentialKey{Key: "get_wrong_pass"}, "wrong-passphrase")
	require.Error(err)
}

func TestGetCredentialStruct(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	input := googleCredentials{
		ClientID:     "client-id-123",
		ClientSecret: "client-secret-456",
		RefreshToken: "1//refresh-token-789",
	}

	_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "get_google"},
		Credentials:   input,
	})
	require.NoError(err)

	got, err := mgr.GetCredential(ctx, schema.CredentialKey{Key: "get_google"}, "test-passphrase")
	require.NoError(err)

	var output googleCredentials
	require.NoError(json.Unmarshal(got, &output))
	require.Equal(input, output)
}

///////////////////////////////////////////////////////////////////////////////
// TESTS - GetCredentialWithoutPassphrase (internal)

func TestGetCredentialWithoutPassphrase(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "get_internal_string"},
		Credentials:   "internal-secret",
	})
	require.NoError(err)

	got, err := mgr.GetCredentialWithoutPassphrase(ctx, schema.CredentialKey{Key: "get_internal_string"})
	require.NoError(err)

	var value string
	require.NoError(json.Unmarshal(got, &value))
	require.Equal("internal-secret", value)
}

func TestGetCredentialWithoutPassphraseStruct(t *testing.T) {
	require := require.New(t)
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	input := googleCredentials{
		ClientID:     "id-abc",
		ClientSecret: "secret-xyz",
		RefreshToken: "refresh-000",
	}

	_, err := mgr.CreateCredential(ctx, schema.CredentialCreate{
		CredentialKey: schema.CredentialKey{Key: "get_internal_google"},
		Credentials:   input,
	})
	require.NoError(err)

	got, err := mgr.GetCredentialWithoutPassphrase(ctx, schema.CredentialKey{Key: "get_internal_google"})
	require.NoError(err)

	var output googleCredentials
	require.NoError(json.Unmarshal(got, &output))
	require.Equal(input, output)
}
