package backend

import (
	"context"
	"io"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	blob "gocloud.dev/blob"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// CreateObject creates an object in the backend
func (b *blobbackend) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	// Validate name
	if req.Name != "" && req.Name != b.Name() {
		return nil, httpresponse.ErrBadRequest.Withf("name %q not handled by backend %q", req.Name, b.Name())
	}

	// Compute key using the request path
	key := b.Key(req.Path)
	if key == "" {
		return nil, httpresponse.ErrBadRequest.Withf("path %q not handled by backend %q", req.Path, b.Name())
	}
	sk := b.storageKey(key)

	// Build metadata, including modtime if set
	meta := req.Meta
	if !req.ModTime.IsZero() {
		if meta == nil {
			meta = make(schema.ObjectMeta)
		}
		meta[filer.AttrLastModified] = req.ModTime.Format(time.RFC3339)
	}

	// Write the object
	if w, err := b.bucket.NewWriter(ctx, sk, &blob.WriterOptions{
		ContentType: req.ContentType,
		Metadata:    meta,
	}); err != nil {
		return nil, blobErr(err, b.Name()+":"+key)
	} else if _, err := io.Copy(w, req.Body); err != nil {
		w.Close()
		b.bucket.Delete(ctx, sk)
		return nil, blobErr(err, b.Name()+":"+key)
	} else if err := w.Close(); err != nil {
		b.bucket.Delete(ctx, sk)
		return nil, blobErr(err, b.Name()+":"+key)
	}

	// Get attributes to return
	attrs, err := b.bucket.Attributes(ctx, sk)
	if err != nil {
		return nil, blobErr(err, b.Name()+":"+key)
	}

	// Return success
	return b.attrsToObject(b.Name(), key, attrs), nil
}
