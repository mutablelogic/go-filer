package aws

import (
	"context"

	// Packages
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	attribute "go.opentelemetry.io/otel/attribute"
)

// GetObject fetches object metadata via HeadObject.
func (b *backend) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)
	ref := b.Name() + ":" + objPath
	addSpanAttrs(ctx,
		attribute.String("s3.path", objPath),
		attribute.String("s3.key", sk),
	)

	head, err := b.client.HeadObject(ctx, &s3svc.HeadObjectInput{
		Bucket: awssdk.String(b.bucket),
		Key:    awssdk.String(sk),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, gofiler.ErrNotFound.Withf("object %q not found", ref)
		}
		return nil, s3Err(err, ref)
	}

	return b.attrsFromHead(objPath, head), nil
}
