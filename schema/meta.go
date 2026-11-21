package schema

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf16"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Meta is a string key-value map for user-defined metadata.
// Keys should be lowercase for S3 compatibility. Text is the value's text,
// for people, such as 5s of a duration whose value is 5000000000, when it
// says more than the value, which is shown in its place.
type Meta struct {
	Namespace string          `json:"namespace,omitempty"`
	Key       string          `json:"key,omitempty"`
	Value     json.RawMessage `json:"value,omitempty"`
	Text      string          `json:"text,omitempty"`
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
// which is a JSON array, which is empty without metadata, whose characters
// which aren't ASCII are escaped, so it's a header's value
func EncodeMetaHeader(meta []Meta) (string, error) {
	if meta == nil {
		meta = []Meta{}
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return ASCIIJSON(string(data)), nil
}

// DecodeMetaHeader returns the user's metadata of the value of MetaHeader,
// which is a JSON array, which can be empty, but not null, each of which has
// a key and a value, which is JSON, which can be null
func DecodeMetaHeader(value string) ([]Meta, error) {
	var meta []Meta
	if strings.TrimSpace(value) == "null" {
		return nil, fmt.Errorf("invalid %s: null, rather than an array", MetaHeader)
	} else if err := json.Unmarshal([]byte(value), &meta); err != nil {
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

////////////////////////////////////////////////////////////////////////////////
// FOLDERS

const (
	// FolderNamespace is the namespace of the metadata of a folder: how
	// many objects which aren't folders, and how many subfolders, it lists
	FolderNamespace = "folder"

	// The keys of a folder's metadata
	FolderObjectCount    = "object-count"
	FolderSubfolderCount = "subfolder-count"
)

// FolderMeta returns the metadata of a folder, of how many objects which
// aren't folders, and how many subfolders, it lists
func FolderMeta(objects, subfolders int) []Meta {
	return []Meta{
		{Namespace: FolderNamespace, Key: FolderObjectCount, Value: json.RawMessage(strconv.Itoa(objects))},
		{Namespace: FolderNamespace, Key: FolderSubfolderCount, Value: json.RawMessage(strconv.Itoa(subfolders))},
	}
}
