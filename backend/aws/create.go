package aws

import (
	"context"
	"time"

	// Packages
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	attribute "go.opentelemetry.io/otel/attribute"
)

// CreateObject uploads an object to S3.
func (b *backend) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)
	ref := b.Name() + ":" + objPath
	addSpanAttrs(ctx,
		attribute.String("s3.path", objPath),
		attribute.String("s3.key", sk),
	)

	// Conditional create: reject if the object already exists.
	if req.IfNotExists {
		_, err := b.client.HeadObject(ctx, &s3svc.HeadObjectInput{
			Bucket: awssdk.String(b.bucket),
			Key:    awssdk.String(sk),
		})
		if err == nil {
			return nil, gofiler.ErrConflict.Withf("object %q already exists", ref)
		}
		if !isNotFound(err) {
			return nil, s3Err(err, ref)
		}
		// NotFound → safe to proceed.
	}

	// Clone metadata so we don't mutate the caller's map.
	meta := make(map[string]string, len(req.Meta)+1)
	for k, v := range req.Meta {
		meta[k] = v
	}
	if !req.ModTime.IsZero() {
		meta[schema.AttrLastModified] = req.ModTime.Format(time.RFC3339)
	}

	input := &s3svc.PutObjectInput{
		Bucket: awssdk.String(b.bucket),
		Key:    awssdk.String(sk),
		Body:   req.Body,
	}
	if req.ContentType != "" {
		input.ContentType = awssdk.String(req.ContentType)
	}
	if len(meta) > 0 {
		input.Metadata = meta
	}

	putOut, err := b.client.PutObject(ctx, input)
	if err != nil {
		return nil, s3Err(err, ref)
	}

	// Fetch final metadata for the response.
	head, err := b.client.HeadObject(ctx, &s3svc.HeadObjectInput{
		Bucket: awssdk.String(b.bucket),
		Key:    awssdk.String(sk),
	})
	if err != nil {
		// Write succeeded but metadata fetch failed — return a partial object
		// to avoid spurious retries that would duplicate the upload.
		obj := &schema.Object{
			Name:        b.Name(),
			Path:        objPath,
			ContentType: req.ContentType,
		}
		if putOut.ETag != nil {
			obj.ETag = normaliseETag(*putOut.ETag)
		}
		return obj, nil
	}

	return b.attrsFromHead(objPath, head), nil
}
