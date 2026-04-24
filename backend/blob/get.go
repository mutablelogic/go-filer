package blob

import (
	"context"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	gcerrors "gocloud.dev/gcerrors"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// GetObject gets object metadata
func (b *backend) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)

	for _, candidate := range b.storageKeyCandidates(sk) {
		attrs, err := b.bucket.Attributes(ctx, candidate)
		if err == nil {
			obj := b.attrsToObject(objPath, attrs)
			obj.Name = b.Name()
			return obj, nil
		}
		if gcerrors.Code(err) == gcerrors.PermissionDenied {
			return nil, blobErr(err, b.Name()+":"+objPath)
		}
	}

	return nil, gofiler.ErrNotFound.Withf("object %q not found", b.Name()+":"+objPath)
}
