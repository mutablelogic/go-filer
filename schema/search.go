package schema

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

// OffsetLimit is a page of what's returned: after Offset of them, of at
// most Limit, or every one, when it's nil, as go-pg's is
type OffsetLimit struct {
	Offset uint64  `json:"offset,omitempty" help:"How many are skipped, before those which are returned"`
	Limit  *uint64 `json:"limit,omitempty" help:"The most which are returned, or every one, when it isn't set"`
}

// SearchRequest is a search of the objects of every volume, of its query,
// and the page of what matches
type SearchRequest struct {
	OffsetLimit
	Query
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
