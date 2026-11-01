//go:build !client

package cmd

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_EnvCredentials(t *testing.T) {
	ctx := context.Background()
	t.Setenv("FILER_TEST_KEY", "AKID")
	t.Setenv("FILER_TEST_EMPTY", "")
	credentials := newEnvCredentials()

	// Placeholders are expanded from the environment, including a variable
	// which is empty, and one which isn't set isn't found
	value, err := credentials.Expand(ctx, "${FILER_TEST_KEY}:$FILER_TEST_KEY:${FILER_TEST_EMPTY}")
	require.NoError(t, err)
	assert.Equal(t, "AKID:AKID:", value)
	_, err = credentials.Expand(ctx, "${FILER_TEST_MISSING}")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	value, err = credentials.Expand(ctx, "plain")
	require.NoError(t, err)
	assert.Equal(t, "plain", value)

	// A credential which is stored replaces the environment's, and isn't
	// written to it
	require.NoError(t, credentials.Set(ctx, "FILER_TEST_KEY", json.RawMessage(`"OTHER"`)))
	require.NoError(t, credentials.Set(ctx, "FILER_TEST_STORED", json.RawMessage(`"secret"`)))
	value, err = credentials.Expand(ctx, "${FILER_TEST_KEY}/${FILER_TEST_STORED}")
	require.NoError(t, err)
	assert.Equal(t, "OTHER/secret", value)
	_, exists := os.LookupEnv("FILER_TEST_STORED")
	assert.False(t, exists)
	assert.ErrorIs(t, credentials.Set(ctx, "FILER_TEST_KEY", json.RawMessage(`1`)), gofiler.ErrBadParameter)

	// A credential is returned as a JSON string
	data, err := credentials.Get(ctx, "FILER_TEST_STORED")
	require.NoError(t, err)
	assert.JSONEq(t, `"secret"`, string(data))
	_, err = credentials.Get(ctx, "FILER_TEST_MISSING")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
}
