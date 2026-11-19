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
}

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
		if term = strings.TrimSpace(term); term == "" {
			continue
		}
		prefix := strings.HasSuffix(term, "*")
		if prefix {
			term = strings.TrimRight(term, "*")
		}
		if strings.ContainsAny(term, " \t\n\r\"'\\") {
			term = `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`, "\r", `\r`, "\t", `\t`).Replace(term) + `"`
		}
		if prefix {
			term += "*"
		}
		words = append(words, term)
	}
	return []byte(strings.Join(words, " ")), nil
}

// UnmarshalText reads the query of text, of its terms, any of which match:
// each word, which is what's between spaces, such as arvo, or
// work@example.com, which is one term, a quoted phrase, such as "spiegel im
// spiegel", and the start of a word, of one which ends with *, such as
// beethov*, or "arvo pa"*. Returns an error of a quote which doesn't end.
func (q *Query) UnmarshalText(text []byte) error {
	scanner := tokenizer.NewScanner(strings.NewReader(string(text)), tokenizer.Pos{})
	var result Query
	var word strings.Builder
	end := func() {
		if term := strings.TrimSpace(word.String()); term != "" && term != "*" {
			result.Terms = append(result.Terms, term)
		}
		word.Reset()
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
		case token.Kind == tokenizer.String:
			// A quoted phrase is a term of its own, which is the start of a
			// word, when * is after it
			end()
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
