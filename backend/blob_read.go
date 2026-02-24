package backend

import (
	"context"
	"io"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ReadObject reads object content
func (b *blobbackend) ReadObject(ctx context.Context, req schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error) {
	// Validate name
	if req.Name != "" && req.Name != b.Name() {
		return nil, nil, httpresponse.ErrBadRequest.Withf("name %q not handled by backend %q", req.Name, b.Name())
	}

	// Compute key using the request path
	key := b.Key(req.Path)
	if key == "" {
		return nil, nil, httpresponse.ErrBadRequest.Withf("path %q not handled by backend %q", req.Path, b.Name())
	}

	// Get and return reader and attributes
	if attrs, err := b.bucket.Attributes(ctx, b.storageKey(key)); err != nil {
		return nil, nil, blobErr(err, b.Name()+":"+key)
	} else if r, err := b.bucket.NewReader(ctx, b.storageKey(key), nil); err != nil {
		return nil, nil, blobErr(err, b.Name()+":"+key)
	} else {
		return r, b.attrsToObject(b.Name(), key, attrs), nil
	}
}
