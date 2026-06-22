package manager

import (
	"context"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) Search(ctx context.Context, req schema.SearchRequest) (_ *schema.SearchResult, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "Search",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Return success
	var result schema.SearchResult
	return types.Ptr(result), nil
}
