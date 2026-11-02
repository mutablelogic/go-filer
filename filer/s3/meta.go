package s3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

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

	// metaModTime is the name of the metadata of an object which is its
	// modification time, as S3 sets its Last-Modified time to when it's
	// written, in seconds since the Unix epoch, with a fraction, such as
	// 1764569708.5, as rclone and s3fs write it. It isn't the user's.
	metaModTime = "mtime"
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
		if name == metaModTime {
			continue
		}
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
	if len(result) == 0 {
		return nil
	}
	return result
}

// s3Metadata returns the user's metadata of an object, as its x-amz-meta-*
// headers, without the prefix, which are named by the key, or the namespace
// and the key, separated by a ".", whose values are JSON, with non-ASCII
// characters escaped, as S3 only stores ASCII, with the modification time,
// when it's set (see metaModTime), which counts towards the size. A
// namespace or key which isn't lowercase letters, numbers and "-", the name
// of the modification time, a value which isn't JSON, a name which is
// repeated, or metadata of more than 2 KB, is ErrBadParameter.
func s3Metadata(meta []schema.Meta, modTime time.Time) (map[string]string, error) {
	if len(meta) == 0 && modTime.IsZero() {
		return nil, nil
	}
	result := make(map[string]string, len(meta)+1)
	size := 0
	if !modTime.IsZero() {
		result[metaModTime] = formatModTime(modTime)
		size += len(metaModTime) + len(result[metaModTime])
	}
	for _, m := range meta {
		name := m.Key
		if !metaName.MatchString(m.Key) {
			return nil, gofiler.ErrBadParameter.Withf("Invalid metadata key %q, which must be lowercase letters, numbers and -", m.Key)
		} else if m.Namespace != "" && !metaName.MatchString(m.Namespace) {
			return nil, gofiler.ErrBadParameter.Withf("Invalid metadata namespace %q, which must be lowercase letters, numbers and -", m.Namespace)
		} else if m.Namespace != "" {
			name = m.Namespace + "." + m.Key
		}
		if name == metaModTime {
			return nil, gofiler.ErrBadParameter.Withf("Metadata %q is the modification time", name)
		} else if _, exists := result[name]; exists {
			return nil, gofiler.ErrBadParameter.Withf("Metadata %q is repeated", name)
		}
		var value bytes.Buffer
		if err := json.Compact(&value, m.Value); err != nil {
			return nil, gofiler.ErrBadParameter.Withf("Metadata %q isn't JSON: %v", name, err)
		}
		result[name] = schema.ASCIIJSON(value.String())
		if size += len(name) + len(result[name]); size > metaSize {
			return nil, gofiler.ErrBadParameter.Withf("Metadata is more than %d bytes", metaSize)
		}
	}
	return result, nil
}

// formatModTime returns a modification time as the value of its metadata,
// in seconds since the Unix epoch, which are negative before it, with the
// fraction of a second, without trailing zeros, when it has one
func formatModTime(t time.Time) string {
	seconds, ns, sign := t.Unix(), int64(t.Nanosecond()), ""
	if seconds < 0 {
		// Before the epoch, the seconds are rounded down, so the fraction is
		// of the second after them
		sign, seconds = "-", -seconds
		if ns != 0 {
			seconds, ns = seconds-1, 1e9-ns
		}
	}
	value := sign + strconv.FormatInt(seconds, 10)
	if ns != 0 {
		value += "." + strings.TrimRight(fmt.Sprintf("%09d", ns), "0")
	}
	return value
}

// parseModTime returns the modification time of the value of its metadata,
// in seconds since the Unix epoch, which are negative before it, with an
// optional fraction of a second, and whether it's one. A time which isn't
// in the years 0 to 9999 isn't one, as it can't be JSON.
func parseModTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	negative := strings.HasPrefix(value, "-")
	whole, fraction, dot := strings.Cut(strings.TrimPrefix(value, "-"), ".")
	if !digits(whole) || len(fraction) > 9 || (dot && !digits(fraction)) {
		return time.Time{}, false
	}
	seconds, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	var ns int64
	if fraction != "" {
		ns, _ = strconv.ParseInt(fraction+strings.Repeat("0", 9-len(fraction)), 10, 64)
	}
	if negative {
		seconds, ns = -seconds, -ns
	}
	t := time.Unix(seconds, ns).UTC()
	if year := t.Year(); year < 0 || year > 9999 {
		return time.Time{}, false
	}
	return t, true
}

// digits returns true when the value is one or more decimal digits
func digits(value string) bool {
	return value != "" && strings.Trim(value, "0123456789") == ""
}
