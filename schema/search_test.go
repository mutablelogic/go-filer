package schema

import (
	"testing"

	// Packages
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_ParseQuery(t *testing.T) {
	for text, want := range map[string][]string{
		"arvo beethoven":                {"arvo", "beethoven"},
		`"spiegel im spiegel" arvo`:     {"spiegel im spiegel", "arvo"},
		"beethov* pärt":                 {"beethov*", "pärt"},
		"work@example.com IMG_2016.jpg": {"work@example.com", "IMG_2016.jpg"},
		"  \t ":                         nil,
		"true null 2016":                {"true", "null", "2016"},
		`a"b c"`:                        {"a", "b c"},
	} {
		query, err := ParseQuery(text)
		require.NoError(t, err, text)
		assert.Equal(t, want, query.Terms, text)
	}
	_, err := ParseQuery(`"not ended`)
	assert.Error(t, err)
}
