package manager

import (
	"context"
	"io"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-media/metadata"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (filer *Filer) GetMetadata(ctx context.Context, r io.Reader) (_ *schema.ObjectMeta, err error) {
	name := "unknown"
	if fr, ok := r.(metadata.NamedStream); ok {
		name = fr.Name()
	}
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "GetMetadata",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Return success
	return filer.metadata.GetMeta(ctx, r)
}
