package manager

import (
	"context"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/credential/schema"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListCredentials returns a paginated list of credentials, excluding the encrypted credential payload.
func (manager *Manager) ListCredentials(ctx context.Context, req schema.CredentialListRequest) (_ *schema.CredentialList, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListCredentials",
		attribute.String("req", req.String()),
	)
	defer func() { endSpan(err) }()

	var result schema.CredentialList
	if err := manager.PoolConn.List(ctx, &result, &req); err != nil {
		return nil, pg.NormalizeError(err)
	} else {
		result.CredentialListRequest = req
		result.OffsetLimit.Clamp(uint64(result.Count))
	}

	// Return success
	return types.Ptr(result), nil
}
