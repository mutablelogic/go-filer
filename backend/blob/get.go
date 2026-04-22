package blob

import (
	"context"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// GetObject gets object metadata
func (b *backend) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)

	attrs, err := b.bucket.Attributes(ctx, sk)
	if err != nil {
		return nil, blobErr(err, b.Name()+":"+objPath)
	}
	obj := b.attrsToObject(objPath, attrs)
	obj.Name = b.Name()
	return obj, nil
}
