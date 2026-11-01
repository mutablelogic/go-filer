package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_MetaHeader(t *testing.T) {
	// The metadata is a JSON array, which is ASCII, and is decoded the same
	meta := []schema.Meta{
		{Namespace: "photo", Key: "caption", Value: json.RawMessage(`"Zoë"`)},
		{Key: "rating", Value: json.RawMessage(`5`)},
	}
	value, err := schema.EncodeMetaHeader(meta)
	require.NoError(t, err)
	assert.Equal(t, `[{"namespace":"photo","key":"caption","value":"Zo\u00eb"},{"key":"rating","value":5}]`, value)
	decoded, err := schema.DecodeMetaHeader(value)
	require.NoError(t, err)
	require.Len(t, decoded, 2)
	assert.Equal(t, "photo", decoded[0].Namespace)
	assert.JSONEq(t, `"Zoë"`, string(decoded[0].Value))
	assert.JSONEq(t, `5`, string(decoded[1].Value))

	// No metadata is an empty array, which is decoded, as is a value which is
	// null
	value, err = schema.EncodeMetaHeader(nil)
	require.NoError(t, err)
	assert.Equal(t, `[]`, value)
	decoded, err = schema.DecodeMetaHeader(value)
	require.NoError(t, err)
	assert.Empty(t, decoded)
	decoded, err = schema.DecodeMetaHeader(`[{"key":"a","value":null}]`)
	require.NoError(t, err)
	assert.JSONEq(t, `null`, string(decoded[0].Value))

	// A value which isn't a JSON array of metadata, each with a key and a
	// value, isn't decoded
	for _, value := range []string{``, `{}`, `null`, ` null `, `[{"key":"a"}]`, `[{"value":1}]`, `[{"key":"a","value":five}]`, `[1]`} {
		_, err := schema.DecodeMetaHeader(value)
		assert.Error(t, err, value)
	}
}

func Test_ASCIIJSON(t *testing.T) {
	// Characters which aren't ASCII, and DEL, are escaped
	assert.Equal(t, `"a\u007fb\u00e9\ud83d\udcf7"`, schema.ASCIIJSON("\"a\x7fbé📷\""))
	assert.Equal(t, `{"a":[1,"b"]}`, schema.ASCIIJSON(`{"a":[1,"b"]}`))
}
