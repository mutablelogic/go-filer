package filer

import (
	"context"
	"errors"
	"io"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// List returns a page of the objects beneath a path in the volume named by
// the request, of at most its limit, which defaults to and is capped at
// schema.ObjectListLimit. When there are more, Next is the key of the last
// object, which the next page starts after, with StartAfter.
func (self *Registry) List(ctx context.Context, req schema.ObjectListRequest) (_ *schema.ObjectList, err error) {
	// Set the limit, without modifying the caller's request
	limit := uint64(schema.ObjectListLimit)
	if req.Limit != nil {
		limit = max(min(*req.Limit, limit), 1)
	}
	req.Limit = types.Ptr(limit)

	// List the objects, reading one more than the limit, to know whether
	// there's a next page
	iter, err := self.ListObjects(ctx, req.ListObjectsRequest)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	list := &schema.ObjectList{ObjectListRequest: req, Body: []*schema.Object{}}

	// The count of the listing, of every page, when it can be counted
	if n, err := count(ctx, iter); err == nil {
		list.Count = types.Ptr(n)
	} else if !errors.Is(err, gofiler.ErrNotImplemented) {
		return nil, err
	}
	for {
		obj, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			return list, nil
		} else if err != nil {
			return nil, err
		} else if uint64(len(list.Body)) == limit {
			list.Next = list.Body[limit-1].Key()
			return list, nil
		}
		list.Body = append(list.Body, obj)
	}
}
