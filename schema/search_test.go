package schema

import (
	"encoding/json"
	"testing"

	// Packages
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_QueryText(t *testing.T) {
	// The terms of text: words between spaces, quoted phrases, and the
	// starts of words
	for text, want := range map[string][]string{
		"arvo beethoven":                {"arvo", "beethoven"},
		`"spiegel im spiegel" arvo`:     {"spiegel im spiegel", "arvo"},
		"beethov* pärt":                 {"beethov*", "pärt"},
		`"arvo pa"* x`:                  {"arvo pa*", "x"},
		"work@example.com IMG_2016.jpg": {"work@example.com", "IMG_2016.jpg"},
		"  \t ":                         nil,
		"true null 2016":                {"true", "null", "2016"},
		`a"b c"`:                        {"a", "b c"},
	} {
		var query Query
		require.NoError(t, query.UnmarshalText([]byte(text)), text)
		assert.Equal(t, want, query.Terms, text)
	}
	var query Query
	assert.Error(t, query.UnmarshalText([]byte(`"not ended`)))

	// Which are written as text, which is read again as they are
	for _, terms := range [][]string{
		{"arvo", "beethov*"},
		{"spiegel im spiegel", "arvo pa*"},
		{`say "hi"`, `back\\slash`, "it's"},
	} {
		text, err := Query{Terms: terms}.MarshalText()
		require.NoError(t, err)
		var read Query
		require.NoError(t, read.UnmarshalText(text), string(text))
		assert.Equal(t, terms, read.Terms, string(text))
	}
	assert.Equal(t, `arvo "spiegel im spiegel" "arvo pa"*`, Query{Terms: []string{"arvo", "spiegel im spiegel", "arvo pa*"}}.String())
}

func Test_QueryJSON(t *testing.T) {
	// A query is a string, of JSON, of its text, which a request has
	req := SearchRequest{Query: Query{Terms: []string{"arvo", "spiegel im spiegel"}}}
	data, err := json.Marshal(req)
	require.NoError(t, err)
	assert.JSONEq(t, `{"query":"arvo \"spiegel im spiegel\""}`, string(data))
	var read SearchRequest
	require.NoError(t, json.Unmarshal(data, &read))
	assert.Equal(t, req, read)

	// Or an array of its terms, as they are, or null
	var query Query
	require.NoError(t, json.Unmarshal([]byte(`["spiegel im spiegel", " ", "beethov*"]`), &query))
	assert.Equal(t, []string{"spiegel im spiegel", "beethov*"}, query.Terms)
	require.NoError(t, json.Unmarshal([]byte(`null`), &query))
	assert.Nil(t, query.Terms)
	assert.Error(t, json.Unmarshal([]byte(`42`), &query))
	assert.Error(t, json.Unmarshal([]byte(`[1]`), &query))
}
