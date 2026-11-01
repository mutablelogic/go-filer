//go:build !client

package cmd

import (
	"os"
	"testing"

	// Packages
	kong "github.com/alecthomas/kong"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_ServerWorker(t *testing.T) {
	parse := func(args ...string) Flags {
		t.Helper()
		var cli struct {
			Flags
		}
		parser, err := kong.New(&cli)
		require.NoError(t, err)
		_, err = parser.Parse(args)
		require.NoError(t, err)
		return cli.Flags
	}

	// The worker is the flag's, or the environment's, FILER_WORKER before
	// WORKER, and is otherwise empty, when it's the hostname
	t.Setenv("FILER_WORKER", "")
	t.Setenv("WORKER", "")
	require.NoError(t, os.Unsetenv("FILER_WORKER"))
	require.NoError(t, os.Unsetenv("WORKER"))
	assert.Empty(t, parse().Worker)
	assert.Equal(t, "worker-1", parse("--worker", "worker-1").Worker)
	t.Setenv("WORKER", "worker-3")
	assert.Equal(t, "worker-3", parse().Worker)
	t.Setenv("FILER_WORKER", "worker-2")
	assert.Equal(t, "worker-2", parse().Worker)
	assert.Equal(t, "worker-1", parse("--worker", "worker-1").Worker)
}
