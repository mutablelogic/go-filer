package manager

import (
	"encoding/json"
	"testing"

	require "github.com/stretchr/testify/require"
)

///////////////////////////////////////////////////////////////////////////////
// HELPERS

func newCryptManager(t *testing.T, opts ...Opt) *Manager {
	t.Helper()
	m := &Manager{}
	if err := m.opt.apply(opts); err != nil {
		t.Fatal(err)
	}
	return m
}

///////////////////////////////////////////////////////////////////////////////
// TESTS - encryptCredentials

func TestEncryptNil(t *testing.T) {
	m := newCryptManager(t, WithPassphrase(1, "passphrase"))
	pv, data, err := m.encryptCredentials(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), pv)
	require.Empty(t, data)
}

func TestEncryptEmptyString(t *testing.T) {
	m := newCryptManager(t, WithPassphrase(1, "passphrase"))
	pv, data, err := m.encryptCredentials("")
	require.NoError(t, err)
	require.Equal(t, uint64(0), pv)
	require.Empty(t, data)
}

func TestEncryptEmptyBytes(t *testing.T) {
	m := newCryptManager(t, WithPassphrase(1, "passphrase"))
	pv, data, err := m.encryptCredentials([]byte{})
	require.NoError(t, err)
	require.Equal(t, uint64(0), pv)
	require.Empty(t, data)
}

func TestEncryptNoPassphrase(t *testing.T) {
	m := newCryptManager(t)
	_, _, err := m.encryptCredentials("secret")
	require.Error(t, err)
}

func TestEncryptString(t *testing.T) {
	m := newCryptManager(t, WithPassphrase(1, "passphrase"))
	pv, data, err := m.encryptCredentials("secret-token")
	require.NoError(t, err)
	require.Equal(t, uint64(1), pv)
	require.NotEmpty(t, data)
}

func TestEncryptStruct(t *testing.T) {
	m := newCryptManager(t, WithPassphrase(1, "passphrase"))
	pv, data, err := m.encryptCredentials(struct {
		Token string `json:"token"`
	}{"abc123"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), pv)
	require.NotEmpty(t, data)
}

///////////////////////////////////////////////////////////////////////////////
// TESTS - decryptCredentials

func TestDecryptEmpty(t *testing.T) {
	m := newCryptManager(t, WithPassphrase(1, "passphrase"))
	var out json.RawMessage
	require.NoError(t, m.decryptCredentials([]byte{}, 1, &out))
	require.Nil(t, out)
}

func TestDecryptNoPassphrase(t *testing.T) {
	// Encrypt with a passphrase, then try to decrypt without one.
	enc := newCryptManager(t, WithPassphrase(1, "passphrase"))
	_, data, err := enc.encryptCredentials("secret")
	require.NoError(t, err)

	dec := newCryptManager(t)
	var out json.RawMessage
	require.Error(t, dec.decryptCredentials(data, 1, &out))
}

func TestDecryptWrongPassphraseVersion(t *testing.T) {
	m := newCryptManager(t, WithPassphrase(1, "passphrase"))
	_, data, err := m.encryptCredentials("secret")
	require.NoError(t, err)

	var out json.RawMessage
	require.Error(t, m.decryptCredentials(data, 99, &out))
}

///////////////////////////////////////////////////////////////////////////////
// TESTS - round-trip

func TestRoundTripString(t *testing.T) {
	m := newCryptManager(t, WithPassphrase(1, "passphrase"))

	pv, data, err := m.encryptCredentials("my-token")
	require.NoError(t, err)

	var out string
	require.NoError(t, m.decryptCredentials(data, pv, &out))
	require.Equal(t, "my-token", out)
}

func TestRoundTripStruct(t *testing.T) {
	type creds struct {
		User string `json:"user"`
		Pass string `json:"pass"`
	}

	m := newCryptManager(t, WithPassphrase(1, "passphrase"))
	input := creds{User: "admin", Pass: "hunter2"}

	pv, data, err := m.encryptCredentials(input)
	require.NoError(t, err)

	var output creds
	require.NoError(t, m.decryptCredentials(data, pv, &output))
	require.Equal(t, input, output)
}

func TestRoundTripMultiplePassphrases(t *testing.T) {
	// Encrypt with pv=1, decrypt with manager that also knows pv=2.
	m := newCryptManager(t, WithPassphrase(1, "old-pass"), WithPassphrase(2, "new-pass"))

	pv, data, err := m.encryptCredentials("secret")
	require.NoError(t, err)
	require.Equal(t, uint64(2), pv) // latest passphrase version used

	var out string
	require.NoError(t, m.decryptCredentials(data, pv, &out))
	require.Equal(t, "secret", out)
}
