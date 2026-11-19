package sqlite

import (
	"context"

	// Packages
	filer "github.com/mutablelogic/go-filer/schema"
	schema "github.com/mutablelogic/go-filer/store/sqlite/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	sqlite "github.com/mutablelogic/go-sqlite"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Search returns the objects of every volume which match the request's
// query, best first, with their volumes' names, of its page, and how many
// match, of every page, which is none of a query without terms
func (self *Store) Search(ctx context.Context, req filer.SearchRequest) (_ *filer.SearchResponse, err error) {
	ctx, end := self.span(ctx, "Search", attribute.String("arg.req", types.Stringify(req)))
	defer func() { end(err) }()
	result := &filer.SearchResponse{SearchRequest: req, Body: []*filer.SearchObject{}}
	if schema.Match(req.Query.Terms) == "" {
		return result, nil
	}
	var count schema.SearchCount
	if err := self.conn.Tx(ctx, func(conn sqlite.Conn) error {
		var hits schema.SearchHits
		if err := conn.List(ctx, &hits, schema.SearchRequest{SearchRequest: req}); err != nil {
			return err
		} else if err := conn.Get(ctx, &count, schema.SearchRequest{SearchRequest: req}); err != nil {
			return err
		}
		for _, hit := range hits {
			var objects schema.ObjectList
			if err := conn.Get(ctx, &objects, schema.ObjectKey{Volume: hit.Volume, Key: hit.Key}); err != nil {
				return err
			}
			objects[0].Name = hit.Volume
			result.Body = append(result.Body, &filer.SearchObject{Object: *objects[0], Rank: count.Rank(hit.Score)})
		}
		return nil
	}); err != nil {
		return nil, storeErr(err)
	}
	result.Count = count.Count
	return result, nil
}
