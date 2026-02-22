package backend

import (
	"context"
	"io"
	"net/url"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	blob "gocloud.dev/blob"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// CreateObject creates an object in the backend
func (b *blobbackend) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	// Parse the URL
	u, err := url.Parse(req.URL)
	if err != nil {
		return nil, err
	}

	// Validate the URL matches this backend
	key := b.Path(u)
	if key == "" {
		return nil, httpresponse.ErrBadRequest.Withf("URL %q not handled by this backend", req.URL)
	}

	// Build metadata, including modtime if set
	meta := req.Meta
	if !req.ModTime.IsZero() {
		if meta == nil {
			meta = make(schema.ObjectMeta)
		}
		meta[AttrLastModified] = req.ModTime.Format(time.RFC3339)
	}

	// Write the object
	if w, err := b.bucket.NewWriter(ctx, key, &blob.WriterOptions{
		ContentType: req.ContentType,
		Metadata:    meta,
	}); err != nil {
		return nil, blobErr(err, req.URL)
	} else if _, err := io.Copy(w, req.Body); err != nil {
		w.Close()
		b.bucket.Delete(ctx, key)
		return nil, blobErr(err, req.URL)
	} else if err := w.Close(); err != nil {
		b.bucket.Delete(ctx, key)
		return nil, blobErr(err, req.URL)
	}

	// Get attributes to return
	attrs, err := b.bucket.Attributes(ctx, key)
	if err != nil {
		return nil, blobErr(err, req.URL)
	}

	// Return success
	return b.attrsToObject(req.URL, attrs), nil
}
