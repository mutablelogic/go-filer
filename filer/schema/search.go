package schema

type SearchRequest struct {
	Query string `json:"query"`
}

type SearchResult struct {
	Results []Object `json:"results"`
}
