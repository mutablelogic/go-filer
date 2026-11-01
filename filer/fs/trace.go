package fs

import (
	"context"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// span starts a span of an operation on an object, such as fs.ListObjects,
// with the volume and the path, when there's a tracer, and returns the
// function which ends it, with the operation's error
func (self *FS) span(ctx context.Context, op, path string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	return otel.StartSpan(self.Tracer, ctx, "fs."+op, append([]attribute.KeyValue{
		attribute.String("filer.volume", self.name),
		attribute.String("filer.path", path),
	}, attrs...)...)
}
