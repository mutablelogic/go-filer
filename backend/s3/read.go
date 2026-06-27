package s3

import (
	"context"
	"io"
	"strings"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Read object content from the backend. Caller must close the returned reader.
func (self *S3Backend) ReadObject(ctx context.Context, req schema.GetObjectRequest) (_ io.ReadCloser, _ *schema.Object, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "s3.ReadObject",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Get the object metadata
	object, err := self.GetObject(ctx, req)
	if err != nil {
		return nil, nil, err
	} else if object.ContentType == schema.ContentTypeDirectory {
		return nil, nil, gofiler.ErrBadParameter.Withf("cannot read content of a directory: %q", req.Path)
	}

	// Stream the object content from S3
	basePrefix := strings.TrimPrefix(strings.TrimSuffix(self.url.Path, "/"), "/")
	out, err := self.client.GetObject(ctx, &s3svc.GetObjectInput{
		Bucket: aws.String(self.url.Host),
		Key:    aws.String(s3KeyFromPath(req.Path, basePrefix)),
	})
	if err != nil {
		return nil, nil, err
	}

	return out.Body, object, nil
}
