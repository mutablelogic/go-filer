package manager

import (
	"context"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/credential/schema"
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
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

	pool := manager.PoolConn
	if req.Rotate != nil {
		_, latestPV := manager.passphrases.Get(0)
		if latestPV == 0 {
			return nil, httpresponse.ErrServiceUnavailable.Withf("no encryption passphrase configured for credentials")
		}
		pool = pool.With("latestpv", latestPV).(pg.PoolConn)
	}

	var result schema.CredentialList
	if err := pool.List(ctx, &result, &req); err != nil {
		return nil, pg.NormalizeError(err)
	} else {
		result.CredentialListRequest = req
		result.OffsetLimit.Clamp(uint64(result.Count))
	}

	// Return success
	return types.Ptr(result), nil
}
