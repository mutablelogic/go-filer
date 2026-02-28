package backend

import (
	"context"
	"io"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ReadObject reads object content
func (b *blobbackend) ReadObject(ctx context.Context, req schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)

	attrs, err := b.bucket.Attributes(ctx, sk)
	if err != nil {
		return nil, nil, blobErr(err, b.Name()+":"+objPath)
	}
	r, err := b.bucket.NewReader(ctx, sk, nil)
	if err != nil {
		return nil, nil, blobErr(err, b.Name()+":"+objPath)
	}
	obj := b.attrsToObject(objPath, attrs)
	obj.Name = b.Name()
	return r, obj, nil
}
