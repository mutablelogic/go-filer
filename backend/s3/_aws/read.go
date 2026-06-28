package aws

import (
	"context"
	"io"
	"time"

	// Packages
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	attribute "go.opentelemetry.io/otel/attribute"
)

// ReadObject streams object content via GetObject.
// The caller must close the returned io.ReadCloser.
func (b *backend) ReadObject(ctx context.Context, req schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)
	ref := b.Name() + ":" + objPath
	addSpanAttrs(ctx,
		attribute.String("s3.path", objPath),
		attribute.String("s3.key", sk),
	)

	out, err := b.client.GetObject(ctx, &s3svc.GetObjectInput{
		Bucket: awssdk.String(b.bucket),
		Key:    awssdk.String(sk),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, nil, gofiler.ErrNotFound.Withf("object %q not found", ref)
		}
		return nil, nil, s3Err(err, ref)
	}

	obj := &schema.Object{
		Volume:      b.Name(),
		Path:        objPath,
		ContentType: awssdk.ToString(out.ContentType),
		Size:        awssdk.ToInt64(out.ContentLength),
	}
	if out.LastModified != nil {
		obj.ModTime = *out.LastModified
	}
	if out.ETag != nil {
		obj.ETag = normaliseETag(*out.ETag)
	}
	if len(out.Metadata) > 0 {
		meta := make(schema.ObjectMeta, len(out.Metadata))
		for k, v := range out.Metadata {
			meta[k] = v
		}
		obj.Meta = meta
		if lm, ok := out.Metadata[schema.AttrLastModified]; ok {
			if t, err := time.Parse(time.RFC3339, lm); err == nil {
				obj.ModTime = t
			}
		}
	}

	return out.Body, obj, nil
}
