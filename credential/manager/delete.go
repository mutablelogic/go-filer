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

// DeleteCredential deletes a credential row by key.
func (m *Manager) DeleteCredential(ctx context.Context, key schema.CredentialKey) (_ *schema.Credential, err error) {
	ctx, endSpan := otel.StartSpan(m.tracer, ctx, "DeleteCredential",
		attribute.String("key", key.Key),
	)
	defer func() { endSpan(err) }()

	var result schema.Credential
	if err := m.PoolConn.Tx(ctx, func(conn pg.Conn) error {
		return conn.Delete(ctx, &result, key)
	}); err != nil {
		return nil, pg.NormalizeError(err)
	}

	// Return success
	return types.Ptr(result), nil
}
