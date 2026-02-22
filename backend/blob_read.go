package backend

import (
	"context"
	"io"
	"net/url"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ReadObject reads object content
func (b *blobbackend) ReadObject(ctx context.Context, req schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error) {
	// Parse the URL
	u, err := url.Parse(req.URL)
	if err != nil {
		return nil, nil, err
	}

	// Validate the URL matches this backend, then get and return reader and attributes
	if key := b.Key(u); key == "" {
		return nil, nil, httpresponse.ErrBadRequest.Withf("URL %q not handled by this backend", req.URL)
	} else if attrs, err := b.bucket.Attributes(ctx, b.storageKey(key)); err != nil {
		return nil, nil, blobErr(err, req.URL)
	} else if r, err := b.bucket.NewReader(ctx, b.storageKey(key), nil); err != nil {
		return nil, nil, blobErr(err, req.URL)
	} else {
		return r, b.attrsToObject(req.URL, attrs), nil
	}
}
