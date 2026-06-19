package manager

import (
	"context"
	"io"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) GetMetadata(ctx context.Context, r io.Reader) (_ *schema.ObjectMeta, err error) {
	name := "unknown"
	if fr, ok := r.(metadata.FileReader); ok {
		name = fr.Name()
	}
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetMetadata",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Return success
	return manager.metadata.GetMeta(ctx, r)
}
