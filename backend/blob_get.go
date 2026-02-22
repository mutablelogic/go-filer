package backend

import (
	"context"
	"net/url"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// GetObject gets object metadata
func (b *blobbackend) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	// Parse the URL
	u, err := url.Parse(req.URL)
	if err != nil {
		return nil, err
	}

	// Validate the URL matches this backend, then get and return attributes
	if key := b.Key(u); key == "" {
		return nil, httpresponse.ErrBadRequest.Withf("URL %q not handled by this backend", req.URL)
	} else if attrs, err := b.bucket.Attributes(ctx, b.storageKey(key)); err != nil {
		return nil, blobErr(err, req.URL)
	} else {
		return b.attrsToObject(req.URL, attrs), nil
	}
}
