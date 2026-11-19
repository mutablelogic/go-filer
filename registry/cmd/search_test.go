package cmd

import (
	"testing"

	// Packages
	kong "github.com/alecthomas/kong"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_SearchCmd(t *testing.T) {
	var cli struct {
		Search SearchCmd `cmd:"" name:"search"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	// The query is every word after the flags, of words which are excluded,
	// which start with -, which aren't flags
	_, err = parser.Parse([]string{"search", "--limit", "5", "beethoven", "-symphony", `"spiegel im spiegel"`})
	require.NoError(t, err)
	assert.Equal(t, []string{"beethoven", "-symphony", `"spiegel im spiegel"`}, cli.Search.Query)
	require.NotNil(t, cli.Search.Limit)
	assert.Equal(t, uint64(5), *cli.Search.Limit)
}
