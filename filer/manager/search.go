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

func (filer *Filer) Search(ctx context.Context, req schema.SearchListRequest) (_ *schema.SearchList, err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "Search",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	var result schema.SearchList
	if err := filer.PoolConn.List(ctx, &result, &req); err != nil {
		return nil, err
	} else {
		result.SearchListRequest = req
		result.OffsetLimit.Clamp(uint64(result.Count))
	}

	// Return success
	return types.Ptr(result), nil
}
