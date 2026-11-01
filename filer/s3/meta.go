package s3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"unicode/utf16"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// metaSize is the most bytes the user's metadata of an object can have,
	// which are its names, without x-amz-meta-, and values
	metaSize = 2 * 1024
)

// metaName is the pattern of a namespace, or a key, of the user's metadata,
// which is lowercase, as S3 lowercases them, without a ".", which separates
// them, or "_", which some proxies don't pass
var metaName = regexp.MustCompile(`^[a-z0-9-]+$`)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// userMeta returns the user's metadata of an object, which are its
// x-amz-meta-* headers, whose names, without the prefix, are the key, or the
// namespace and the key, separated by a ".", in the order of their names,
// whose values are JSON, or JSON strings, when they aren't JSON, as metadata
// another client wrote, or nil when there's none
func userMeta(metadata map[string]string) []schema.Meta {
	if len(metadata) == 0 {
		return nil
	}
	result := make([]schema.Meta, 0, len(metadata))
	for _, name := range slices.Sorted(maps.Keys(metadata)) {
		meta := schema.Meta{Key: name}
		if namespace, key, ok := strings.Cut(name, "."); ok {
			meta.Namespace, meta.Key = namespace, key
		}
		if value := []byte(metadata[name]); json.Valid(value) {
			meta.Value = value
		} else {
			meta.Value, _ = json.Marshal(metadata[name])
		}
		result = append(result, meta)
	}
	return result
}

// s3Metadata returns the user's metadata of an object, as its x-amz-meta-*
// headers, without the prefix, which are named by the key, or the namespace
// and the key, separated by a ".", whose values are JSON, with non-ASCII
// characters escaped, as S3 only stores ASCII. A namespace or key which
// isn't lowercase letters, numbers and "-", a value which isn't JSON, a name
// which is repeated, or metadata of more than 2 KB, is ErrBadParameter.
func s3Metadata(meta []schema.Meta) (map[string]string, error) {
	if len(meta) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(meta))
	size := 0
	for _, m := range meta {
		name := m.Key
		if !metaName.MatchString(m.Key) {
			return nil, gofiler.ErrBadParameter.Withf("Invalid metadata key %q, which must be lowercase letters, numbers and -", m.Key)
		} else if m.Namespace != "" && !metaName.MatchString(m.Namespace) {
			return nil, gofiler.ErrBadParameter.Withf("Invalid metadata namespace %q, which must be lowercase letters, numbers and -", m.Namespace)
		} else if m.Namespace != "" {
			name = m.Namespace + "." + m.Key
		}
		if _, exists := result[name]; exists {
			return nil, gofiler.ErrBadParameter.Withf("Metadata %q is repeated", name)
		}
		var value bytes.Buffer
		if err := json.Compact(&value, m.Value); err != nil {
			return nil, gofiler.ErrBadParameter.Withf("Metadata %q isn't JSON: %v", name, err)
		}
		result[name] = asciiJSON(value.String())
		if size += len(name) + len(result[name]); size > metaSize {
			return nil, gofiler.ErrBadParameter.Withf("Metadata is more than %d bytes", metaSize)
		}
	}
	return result, nil
}

// asciiJSON returns the JSON, with its non-ASCII characters, which are only
// in its strings, escaped, as \uXXXX, or a surrogate pair
func asciiJSON(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r < 0x80 {
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
