package backend

import (
	"context"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// GetObject gets object metadata
func (b *blobbackend) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	// Validate name
	if req.Name != "" && req.Name != b.Name() {
		return nil, httpresponse.ErrBadRequest.Withf("name %q not handled by backend %q", req.Name, b.Name())
	}

	// Compute key using the request path
	key := b.Key(req.Path)
	if key == "" {
		return nil, httpresponse.ErrBadRequest.Withf("path %q not handled by backend %q", req.Path, b.Name())
	}

	// Get and return attributes
	if attrs, err := b.bucket.Attributes(ctx, b.storageKey(key)); err != nil {
		return nil, blobErr(err, b.Name()+":"+key)
	} else {
		return b.attrsToObject(b.Name(), key, attrs), nil
	}
}
