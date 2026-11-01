package schema

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf16"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Meta is a string key-value map for user-defined metadata.
// Keys should be lowercase for S3 compatibility
type Meta struct {
	Namespace string          `json:"namespace,omitempty"`
	Key       string          `json:"key,omitempty"`
	Value     json.RawMessage `json:"value,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// MetaHeader is the header of a request which writes a file, or of a part of
// a form which uploads files, with the user's metadata of the file, as a
// JSON array of its namespaces, keys and values
const MetaHeader = "X-Meta-Data"

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// EncodeMetaHeader returns the user's metadata as the value of MetaHeader,
// which is a JSON array, whose characters which aren't ASCII are escaped, so
// it's a header's value
func EncodeMetaHeader(meta []Meta) (string, error) {
	data, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return ASCIIJSON(string(data)), nil
}

// DecodeMetaHeader returns the user's metadata of the value of MetaHeader,
// which is a JSON array, each of which has a key and a value, which is JSON
func DecodeMetaHeader(value string) ([]Meta, error) {
	var meta []Meta
	if err := json.Unmarshal([]byte(value), &meta); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", MetaHeader, err)
	}
	for _, m := range meta {
		if m.Key == "" {
			return nil, fmt.Errorf("invalid %s: metadata without a key", MetaHeader)
		} else if len(m.Value) == 0 {
			return nil, fmt.Errorf("invalid %s: metadata %q without a value", MetaHeader, m.Key)
		}
	}
	return meta, nil
}

// ASCIIJSON returns the JSON, with its characters which aren't ASCII, which
// are only in its strings, escaped, as \uXXXX, or a surrogate pair, and DEL,
// which isn't a character of a header's value
func ASCIIJSON(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r < 0x7F {
			b.WriteRune(r)
		} else if r > 0xFFFF {
			r1, r2 := utf16.EncodeRune(r)
			fmt.Fprintf(&b, `\u%04x\u%04x`, r1, r2)
		} else {
			fmt.Fprintf(&b, `\u%04x`, r)
		}
	}
	return b.String()
}
