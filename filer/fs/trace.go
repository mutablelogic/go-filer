package fs

import (
	"context"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// span starts a span of an operation on an object, such as fs.ListObjects,
// with the volume, and the request, as arg.req, when there's a tracer, and
// returns the function which ends it, with the operation's error
func (self *FS) span(ctx context.Context, op string, req any) (context.Context, func(error)) {
	return otel.StartSpan(self.Tracer, ctx, "fs."+op,
		attribute.String("filer.volume", self.name),
		attribute.String("arg.req", types.Stringify(req)),
	)
}
