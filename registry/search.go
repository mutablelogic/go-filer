package registry

import (
	"context"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Search returns the objects of the volumes which are mounted, which match
// the query, of its text (see schema.Query), best first, of the page, whose
// limit defaults to, and is capped at, schema.SearchLimit, and how many
// match, of every page. Returns ErrBadParameter of text which isn't a query,
// such as of a quote which doesn't end.
func (self *Registry) Search(ctx context.Context, text string, page schema.OffsetLimit) (*schema.SearchResponse, error) {
	var query schema.Query
	if err := query.UnmarshalText([]byte(text)); err != nil {
		return nil, gofiler.ErrBadParameter.Withf("%v", err)
	}

	// The page, of its limit, without changing the caller's
	limit := uint64(schema.SearchLimit)
	if page.Limit != nil {
		limit = max(min(*page.Limit, limit), 1)
	}
	page.Limit = types.Ptr(limit)
	req := schema.SearchRequest{OffsetLimit: page, Query: query}

	// Of the volumes which are mounted, of which there may be none, which
	// isn't every one
	if req.Query.Volumes = self.Keys(); len(req.Query.Volumes) == 0 {
		return &schema.SearchResponse{SearchRequest: req, Body: []*schema.SearchObject{}}, nil
	}
	return self.store.Search(ctx, req)
}
