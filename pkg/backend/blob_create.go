package backend

import (
	"context"
	"errors"
	"io"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	blob "gocloud.dev/blob"
	gcerrors "gocloud.dev/gcerrors"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// CreateObject creates an object in the backend
func (b *blobbackend) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)

	// Conditional create: reject if the object already exists
	if req.IfNotExists {
		_, err := b.bucket.Attributes(ctx, sk)
		if err == nil {
			return nil, httpresponse.ErrConflict.Withf("object %q already exists", b.Name()+":"+objPath)
		} else if gcerrors.Code(err) != gcerrors.NotFound {
			return nil, blobErr(err, b.Name()+":"+objPath)
		}
		// NotFound → safe to create
	}

	// Clone metadata to avoid mutating the caller's map
	var meta schema.ObjectMeta
	if req.Meta != nil || !req.ModTime.IsZero() {
		meta = make(schema.ObjectMeta, len(req.Meta)+1)
		for k, v := range req.Meta {
			meta[k] = v
		}
	}
	if !req.ModTime.IsZero() {
		meta[schema.AttrLastModified] = req.ModTime.Format(time.RFC3339)
	}

	// Write the object
	if w, err := b.bucket.NewWriter(ctx, sk, &blob.WriterOptions{
		ContentType: req.ContentType,
		Metadata:    meta,
	}); err != nil {
		return nil, blobErr(err, b.Name()+":"+objPath)
	} else if _, err := io.Copy(w, req.Body); err != nil {
		err = errors.Join(err, w.Close())
		b.bucket.Delete(ctx, sk)
		return nil, blobErr(err, b.Name()+":"+objPath)
	} else if err := w.Close(); err != nil {
		b.bucket.Delete(ctx, sk)
		return nil, blobErr(err, b.Name()+":"+objPath)
	}

	// Get attributes to return
	attrs, err := b.bucket.Attributes(ctx, sk)
	if err != nil {
		// The write succeeded but we couldn't fetch the final metadata.
		// Return a partial object rather than an error to avoid spurious retries
		// that would duplicate the object in storage.
		obj := &schema.Object{
			Name:        b.Name(),
			Path:        objPath,
			ContentType: req.ContentType,
		}
		return obj, nil
	}

	// Return success
	obj := b.attrsToObject(objPath, attrs)
	obj.Name = b.Name()
	return obj, nil
}
