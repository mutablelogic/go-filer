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
		"Schleswig-Holstein":            {"Schleswig-Holstein"},
	} {
		var query Query
		require.NoError(t, query.UnmarshalText([]byte(text)), text)
		assert.Equal(t, want, query.Terms, text)
	}

	// And those which are excluded, of a word which starts with -
	var excluded Query
	require.NoError(t, excluded.UnmarshalText([]byte(`beethoven -symphony -"im spiegel" -op* - x`)))
	assert.Equal(t, []string{"beethoven", "x"}, excluded.Terms)
	assert.Equal(t, []string{"symphony", "im spiegel", "op*"}, excluded.Exclude)
	var query Query
	assert.Error(t, query.UnmarshalText([]byte(`"not ended`)))

	// Which are written as text, which is read again as they are
	for _, query := range []Query{
		{Terms: []string{"arvo", "beethov*"}},
		{Terms: []string{"spiegel im spiegel", "arvo pa*"}},
		{Terms: []string{`say "hi"`, `back\\slash`, "it's", "-dash"}},
		{Terms: []string{"beethoven"}, Exclude: []string{"symphony", "im spiegel", "op*"}},
	} {
		text, err := query.MarshalText()
		require.NoError(t, err)
		var read Query
		require.NoError(t, read.UnmarshalText(text), string(text))
		assert.Equal(t, query, read, string(text))
	}
	assert.Equal(t, `beethoven -symphony -"im spiegel"`, Query{Terms: []string{"beethoven"}, Exclude: []string{"symphony", "im spiegel"}}.String())
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
