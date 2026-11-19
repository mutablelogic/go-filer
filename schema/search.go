package schema

import (
	"encoding"
	"encoding/json"
	"fmt"
	"strings"

	// Packages
	tokenizer "github.com/mutablelogic/go-tokenizer"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Query is a search of objects, of terms, any of which match an object, of
// its name, folders and metadata
type Query struct {
	// Terms are what's searched for, any of which match, each of which is a
	// word, or words, which are next to each other, or the start of a word,
	// of a term which ends with *
	Terms []string `json:"terms,omitempty" help:"What's searched for, any of which match, such as \"arvo part\", or the start of a word, such as beethov*"`

	// Exclude are terms, as Terms are, which an object which matches
	// doesn't, which are of the text's words which start with -, such as
	// -symphony, which a query without terms has no use of
	Exclude []string `json:"exclude,omitempty" help:"What an object which matches doesn't, such as symphony, of -symphony"`

	// Volumes are the volumes which are searched, or every one, without
	// any, which isn't of the query's text, as the registry sets it, of the
	// volumes which are mounted
	Volumes []string `json:"-"`
}

// SearchLimit is the most objects of a page of a search, which is the
// limit, unless a request sets fewer
const SearchLimit = 100

var _ encoding.TextMarshaler = Query{}
var _ encoding.TextUnmarshaler = (*Query)(nil)
var _ json.Marshaler = Query{}
var _ json.Unmarshaler = (*Query)(nil)

// OffsetLimit is a page of what's returned: after Offset of them, of at
// most Limit, or every one, when it's nil, as go-pg's is
type OffsetLimit struct {
	Offset uint64  `json:"offset,omitempty" help:"How many are skipped, before those which are returned"`
	Limit  *uint64 `json:"limit,omitempty" help:"The most which are returned, or every one, when it isn't set"`
}

// SearchRequest is a search of the objects of every volume, of its query,
// which is text, of JSON, and the page of what matches
type SearchRequest struct {
	OffsetLimit
	Query Query `json:"query" help:"What's searched for, such as arvo \"spiegel im spiegel\" beethov*"`
}

// SearchResponse is a page of the objects which match a search, best first,
// with their volumes' names, and how many match, of every page
type SearchResponse struct {
	SearchRequest
	Count uint64          `json:"count" help:"How many objects match, of every page"`
	Body  []*SearchObject `json:"body"`
}

// SearchObject is an object which matches a search, and how well, of 0 to
// 1, which is 1 of the best of the search's, of every page, so ranks are
// of the search, rather than of a page
type SearchObject struct {
	Object
	Rank float32 `json:"rank" help:"How well it matches, of 0 to 1, which is 1 of the best match"`
}

////////////////////////////////////////////////////////////////////////////////
// TEXT

// MarshalText returns the text of the query, of its terms, between spaces,
// each of which is quoted, when it has a space, a quote, or a backslash,
// which are escaped, as a phrase, with * after the quote, of the start of a
// word, so it's read again, of UnmarshalText, as it is
func (q Query) MarshalText() ([]byte, error) {
	var words []string
	for _, term := range q.Terms {
		if word := queryWord(term); word != "" {
			words = append(words, word)
		}
	}
	for _, term := range q.Exclude {
		if word := queryWord(term); word != "" {
			words = append(words, "-"+word)
		}
	}
	return []byte(strings.Join(words, " ")), nil
}

// queryWord returns the word of a term, of a query's text, which is quoted,
// when it has a space, a quote, or a backslash, which are escaped, or when
// it starts with -, which is of an excluded term, with * after the quote, of
// the start of a word, or none, of a term which is empty
func queryWord(term string) string {
	if term = strings.TrimSpace(term); term == "" {
		return ""
	}
	prefix := strings.HasSuffix(term, "*")
	if prefix {
		term = strings.TrimRight(term, "*")
	}
	if strings.ContainsAny(term, " \t\n\r\"'\\") || strings.HasPrefix(term, "-") {
		term = `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`, "\r", `\r`, "\t", `\t`).Replace(term) + `"`
	}
	if prefix {
		term += "*"
	}
	return term
}

// UnmarshalText reads the query of text, of its terms, any of which match:
// each word, which is what's between spaces, such as arvo, or
// work@example.com, which is one term, a quoted phrase, such as "spiegel im
// spiegel", and the start of a word, of one which ends with *, such as
// beethov*, or "arvo pa"*, and those which an object doesn't match, of a
// word which starts with -, such as -symphony, or -"im spiegel", but not a
// hyphen within a word, such as Schleswig-Holstein. Returns an error of a
// quote which doesn't end.
func (q *Query) UnmarshalText(text []byte) error {
	scanner := tokenizer.NewScanner(strings.NewReader(string(text)), tokenizer.Pos{})
	var result Query
	var word strings.Builder
	exclude := false
	end := func() {
		if term := strings.TrimSpace(word.String()); term != "" && term != "*" {
			if exclude {
				result.Exclude = append(result.Exclude, term)
			} else {
				result.Terms = append(result.Terms, term)
			}
		}
		word.Reset()
		exclude = false
	}
	for {
		token := scanner.Next()
		switch {
		case token == nil || token.Kind == tokenizer.EOF:
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("invalid query %q: %w", text, err)
			}
			end()
			*q = result
			return nil
		case token.Kind == tokenizer.Space:
			end()
		case token.Kind == tokenizer.Minus && word.Len() == 0 && !exclude:
			// A word which starts with - is excluded
			exclude = true
		case token.Kind == tokenizer.String:
			// A quoted phrase is a term of its own, which is the start of a
			// word, when * is after it, which is excluded, of - before it
			if word.Len() > 0 {
				end()
			}
			word.WriteString(token.Val)
			if next := scanner.Peak(); next != nil && next.Kind == tokenizer.Multiply {
				scanner.Next()
				word.WriteString("*")
			}
			end()
		default:
			word.WriteString(token.Val)
		}
	}
}

// MarshalJSON returns the query as a string, of its text
func (q Query) MarshalJSON() ([]byte, error) {
	text, err := q.MarshalText()
	if err != nil {
		return nil, err
	}
	return json.Marshal(string(text))
}

// UnmarshalJSON reads the query of a string, of its text, or an array of
// its terms, each of which is a term as it is, or null, which has none
func (q *Query) UnmarshalJSON(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch v := v.(type) {
	case nil:
		*q = Query{}
		return nil
	case string:
		return q.UnmarshalText([]byte(v))
	case []any:
		var result Query
		for _, term := range v {
			s, ok := term.(string)
			if !ok {
				return fmt.Errorf("invalid query: a term of %v, rather than a string", term)
			} else if s = strings.TrimSpace(s); s != "" {
				result.Terms = append(result.Terms, s)
			}
		}
		*q = result
		return nil
	default:
		return fmt.Errorf("invalid query: %s, rather than a string, or an array of terms", data)
	}
}

// String returns the text of the query
func (q Query) String() string {
	text, _ := q.MarshalText()
	return string(text)
}
