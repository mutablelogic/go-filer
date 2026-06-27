package s3

import (
	"context"
	"errors"
	"path"
	"strings"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	mime "github.com/mutablelogic/go-filer/metadata/mime"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Get object metadata from the backend
func (self *S3Backend) GetObject(ctx context.Context, req schema.GetObjectRequest) (_ *schema.Object, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "s3.GetObject",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Head the object to get its metadata
	basePrefix := strings.TrimPrefix(strings.TrimSuffix(self.url.Path, "/"), "/")
	key := s3KeyFromPath(req.Path, basePrefix)
	out, err := self.client.HeadObject(ctx, &s3svc.HeadObjectInput{
		Bucket: aws.String(self.url.Host),
		Key:    aws.String(key),
	})
	if err != nil {
		if s3IsNotFound(err) {
			return nil, gofiler.ErrNotFound.Withf("object not found: %q", req.Path)
		}
		return nil, err
	}

	// Determine the content type, using the S3 metadata if present, or falling back to the file extension.
	contentType := aws.ToString(out.ContentType)
	if contentType == "" {
		contentType = mime.TypeByExtension(path.Ext(req.Path))
	}

	// Return the object metadata
	obj := &schema.Object{
		ObjectKey: schema.ObjectKey{
			Volume: self.Name(),
			Path:   req.Path,
		},
		ObjectMeta: schema.ObjectMeta{
			ContentType: contentType,
		},
		ObjectAttr: schema.ObjectAttr{
			Size: aws.ToInt64(out.ContentLength),
			ETag: stripETagQuotes(out.ETag),
		},
	}
	if out.LastModified != nil {
		obj.ModTime = *out.LastModified
	}
	for k, v := range out.Metadata {
		obj.Meta = schema.AppendMeta(obj.Meta, k, v)
	}

	return obj, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// s3KeyFromPath converts a backend-relative path to an S3 object key.
func s3KeyFromPath(reqPath, basePrefix string) string {
	p := strings.TrimPrefix(path.Clean("/"+strings.TrimSpace(reqPath)), "/")
	if basePrefix != "" {
		return basePrefix + "/" + p
	}
	return p
}

// s3IsNotFound returns true when err represents a missing S3 object (404).
// HeadObject returns a raw HTTP 404 rather than a typed NoSuchKey error.
func s3IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	var noSuchKey *s3types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	type httpCoder interface{ HTTPStatusCode() int }
	var he httpCoder
	if errors.As(err, &he) {
		return he.HTTPStatusCode() == 404
	}
	return false
}
