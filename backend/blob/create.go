package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	blob "gocloud.dev/blob"
	gcerrors "gocloud.dev/gcerrors"
)

// deleteTimeout is the maximum time allowed for a best-effort rollback delete
// after a failed upload. It is intentionally independent of the request context,
// which may already be cancelled (e.g. CTRL+C).
const deleteTimeout = 30 * time.Second

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// CreateObject creates an object in the backend
func (b *backend) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)

	// Conditional create: reject if the object already exists
	if req.IfNotExists {
		_, err := b.bucket.Attributes(ctx, sk)
		if err == nil {
			return nil, fmt.Errorf("%w: %w",
				schema.ErrAlreadyExists,
				gofiler.ErrConflict.Withf("object %q already exists", b.Name()+":"+objPath),
			)
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
		// Use a detached context for rollback: the request context may already be
		// cancelled (e.g. CTRL+C), but we still need to delete the partial upload.
		rollbackCtx, cancel := context.WithTimeout(context.Background(), deleteTimeout)
		defer cancel()
		b.bucket.Delete(rollbackCtx, sk)
		return nil, blobErr(err, b.Name()+":"+objPath)
	} else if err := w.Close(); err != nil {
		// Same: roll back with a fresh context so cancellation doesn't prevent cleanup.
		rollbackCtx, cancel := context.WithTimeout(context.Background(), deleteTimeout)
		defer cancel()
		b.bucket.Delete(rollbackCtx, sk)
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
