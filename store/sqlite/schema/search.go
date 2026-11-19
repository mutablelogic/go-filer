package schema

import (
	"encoding/json"
	"strings"
	"unicode/utf8"

	// Packages
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-sqlite"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// SearchRefresh is the search of the objects of a volume, of the range of
// their keys, from one, to one after, which is refreshed, of what they are
// now
type SearchRefresh struct {
	Volume string
	From   string
	To     string
}

// SearchMatch selects the keys of the objects of a volume, which match an
// FTS5 query, best first, of the weights of their columns, up to a limit
type SearchMatch struct {
	Volume string
	Match  string
	Limit  int
}

// SearchKeys is the keys of the objects which match a search, best first
type SearchKeys []string

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// minPathSegment is the fewest characters of a folder's name which is
	// searched, of an object's path, as shorter ones, such as "a", are noise
	minPathSegment = 3

	// maxSearchText is the most of a value's text which is searched, as of a
	// long description, which is cut at a character
	maxSearchText = 32 << 10
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Functions returns the options of the functions which the queries of the
// search of objects call, of each connection
func Functions() []sqlite.Opt {
	return []sqlite.Opt{
		sqlite.WithFunction("search_name", true, sqlite.Func1(func(key string) (string, error) { return searchName(key), nil })),
		sqlite.WithFunction("search_folder", true, sqlite.Func2(func(key string, depth int64) (string, error) { return searchFolder(key, int(depth)), nil })),
		sqlite.WithFunction("search_path", true, sqlite.Func1(func(key string) (string, error) { return searchPath(key), nil })),
		sqlite.WithFunction("search_text", true, sqlite.Func1(func(value string) (string, error) { return searchText(value), nil })),
	}
}

// //////////////////////////////////////////////////////////////////////////////
// KeyRefresh returns the refresh of the search of an object, of its key
func KeyRefresh(volume, key string) SearchRefresh {
	return SearchRefresh{Volume: volume, From: key, To: key + "\x00"}
}

// VolumeRefresh returns the refresh of the search of every object of a
// volume
func VolumeRefresh(volume string) SearchRefresh {
	from, to := store.Subtree("/")
	return SearchRefresh{Volume: volume, From: from, To: to}
}

// Insert binds the refresh of the search of the objects
func (r SearchRefresh) Insert(bind *sqlite.Bind) (string, error) {
	bind.Set("volume", r.Volume)
	bind.Set("from", r.From)
	bind.Set("to", r.To)
	return bind.Query("search.refresh"), nil
}

func (r SearchRefresh) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("the search is refreshed")
}

// Select binds the match of the search
func (m SearchMatch) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.List {
		return "", sqlite.ErrNotImplemented.Withf("search: %v", op)
	}
	limit := m.Limit
	if limit <= 0 {
		limit = -1
	}
	bind.Set("volume", m.Volume)
	bind.Set("match", m.Match)
	bind.Set("limit", limit)
	return bind.Query("search.match"), nil
}

// Scan reads the key of an object which matches
func (keys *SearchKeys) Scan(row sqlite.Row) error {
	var key string
	if err := row.Scan(&key); err != nil {
		return err
	}
	*keys = append(*keys, key)
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// searchName returns the name of the object of a key, such as b.txt of
// /a/b.txt, or b of the folder /a/b/, or none, of the root
func searchName(key string) string {
	p, _ := store.PathOf(key)
	if p == "/" {
		return ""
	}
	return p[strings.LastIndex(p, "/")+1:]
}

// searchFolders returns the names of the folders of the object of a key,
// nearest first, such as Bern, then Photos, of /Photos/Bern/a.jpg, or
// /Photos/Bern/a/, but not of the object itself, nor the root
func searchFolders(key string) []string {
	parent := strings.Trim(store.ParentKey(key), "/")
	if parent == "" {
		return nil
	}
	names := strings.Split(parent, "/")
	for i, j := 0, len(names)-1; i < j; i, j = i+1, j-1 {
		names[i], names[j] = names[j], names[i]
	}
	return names
}

// searched returns the name of a folder which is searched, which is of
// minPathSegment characters or more, as shorter ones, such as "a", are
// noise, or none
func searched(name string) string {
	if utf8.RuneCountInString(strings.TrimSpace(name)) < minPathSegment {
		return ""
	}
	return name
}

// searchFolder returns the name of a folder of the object of a key, of its
// depth, which is 1 of its own folder, and 2 of the folder above it, which
// are searched apart, of their weights, or none
func searchFolder(key string, depth int) string {
	if names := searchFolders(key); depth >= 1 && depth <= len(names) {
		return searched(names[depth-1])
	}
	return ""
}

// searchPath returns the names of the folders above the two nearest of the
// object of a key, such as "Photos" of /Photos/2026/Bern/a.jpg
func searchPath(key string) string {
	var result []string
	if names := searchFolders(key); len(names) > 2 {
		for _, name := range names[2:] {
			if name = searched(name); name != "" {
				result = append(result, name)
			}
		}
	}
	return strings.Join(result, " ")
}

// searchText returns the text of a metadata value, of JSON: a string, or
// the strings of an array, or an object, such as a calendar's events, but
// not numbers, or booleans, nor links, which are noise, such as
// https://www.google.com/calendar/event?eid=..., up to maxSearchText
func searchText(value string) string {
	var v any
	if err := json.Unmarshal([]byte(value), &v); err != nil {
		return ""
	}
	var b strings.Builder
	var walk func(any)
	walk = func(v any) {
		if b.Len() >= maxSearchText {
			return
		}
		switch v := v.(type) {
		case string:
			if v = strings.TrimSpace(v); v == "" || isLink(v) {
				return
			}
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(v)
		case []any:
			for _, e := range v {
				walk(e)
			}
		case map[string]any:
			for _, e := range v {
				walk(e)
			}
		}
	}
	walk(v)
	text := b.String()
	if len(text) > maxSearchText {
		text = text[:maxSearchText]
		for !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
	}
	return text
}

// isLink returns true of a value which is a link, such as https://..., or
// mailto:..., which isn't searched
func isLink(value string) bool {
	for _, scheme := range []string{"http://", "https://", "mailto:", "file://", "data:"} {
		if len(value) >= len(scheme) && strings.EqualFold(value[:len(scheme)], scheme) {
			return true
		}
	}
	return false
}
