package blob

import (
	"context"
	"io"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	attribute "go.opentelemetry.io/otel/attribute"
	gcerrors "gocloud.dev/gcerrors"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ReadObject reads object content
func (b *backend) ReadObject(ctx context.Context, req schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)
	candidates := b.storageKeyCandidates(sk)
	addSpanAttrs(ctx,
		attribute.String("blob.path", objPath),
		attribute.String("blob.storage_key", sk),
		attribute.String("blob.storage_candidates", strings.Join(candidates, ",")),
	)

	for _, candidate := range candidates {
		attrs, err := b.bucket.Attributes(ctx, candidate)
		if err != nil {
			if gcerrors.Code(err) == gcerrors.PermissionDenied {
				return nil, nil, blobErr(err, b.Name()+":"+objPath)
			}
			continue
		}
		addSpanAttrs(ctx, attribute.String("blob.storage_hit_key", candidate))
		r, err := b.bucket.NewReader(ctx, candidate, nil)
		if err != nil {
			return nil, nil, blobErr(err, b.Name()+":"+objPath)
		}
		obj := b.attrsToObject(objPath, attrs)
		obj.Name = b.Name()
		return r, obj, nil
	}

	return nil, nil, gofiler.ErrNotFound.Withf("object %q not found", b.Name()+":"+objPath)
}
