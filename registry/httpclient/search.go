package httpclient

import (
	"context"
	"net/url"
	"strconv"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Search returns the objects of the mounted volumes which match the query,
// of its text, best first, of the page, and how many match, of every page
// (see schema.Query)
func (c *Client) Search(ctx context.Context, query string, page schema.OffsetLimit) (*schema.SearchResponse, error) {
	params := url.Values{"query": {query}}
	if page.Offset > 0 {
		params.Set("offset", strconv.FormatUint(page.Offset, 10))
	}
	if page.Limit != nil {
		params.Set("limit", strconv.FormatUint(*page.Limit, 10))
	}
	var response schema.SearchResponse
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("search"), client.OptQuery(params)); err != nil {
		return nil, err
	}
	return &response, nil
}
