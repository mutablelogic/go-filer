package blob

import (
	"context"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	attribute "go.opentelemetry.io/otel/attribute"
	gcerrors "gocloud.dev/gcerrors"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// GetObject gets object metadata
func (b *backend) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
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
		if err == nil {
			addSpanAttrs(ctx, attribute.String("blob.storage_hit_key", candidate))
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
