package s3

import (
	"context"

	// Packages
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	middleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	otel "github.com/mutablelogic/go-client/pkg/otel"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// span starts a span of an operation on an object, such as s3.ListObjects,
// with the volume, and the request, as arg.req, when there's a tracer, and
// returns the function which ends it, with the operation's error. The spans
// of the requests to S3 are within it.
func (self *S3) span(ctx context.Context, op string, req any) (context.Context, func(error)) {
	return otel.StartSpan(self.Tracer, ctx, "s3."+op,
		attribute.String("filer.volume", self.name),
		attribute.String("arg.req", types.Stringify(req)),
	)
}

// traceRequests returns the option of a client which traces each request,
// with a span which is named by its operation, such as S3.ListObjectsV2,
// with the bucket, region, and status of its response
func (self *S3) traceRequests(region string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("filer.trace", func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
			service, operation := awsmiddleware.GetServiceID(ctx), awsmiddleware.GetOperationName(ctx)
			ctx, end := otel.StartSpan(self.Tracer, ctx, service+"."+operation,
				attribute.String("rpc.system", "aws-api"),
				attribute.String("rpc.service", service),
				attribute.String("rpc.method", operation),
				attribute.String("cloud.region", region),
				attribute.String("aws.s3.bucket", self.bucket),
			)
			out, metadata, err := next.HandleInitialize(ctx, in)
			if response, ok := awsmiddleware.GetRawResponse(metadata).(*smithyhttp.Response); ok {
				trace.SpanFromContext(ctx).SetAttributes(attribute.Int("http.response.status_code", response.StatusCode))
			}
			end(err)
			return out, metadata, err
		}), middleware.After)
	}
}
