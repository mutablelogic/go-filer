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

// CreateCredential persists an encrypted credential row and returns the public
// credential shape, excluding passphrase version and encrypted payload.
func (m *Manager) CreateCredential(ctx context.Context, req schema.CredentialCreate) (_ *schema.Credential, err error) {
	ctx, endSpan := otel.StartSpan(m.tracer, ctx, "CreateCredential",
		attribute.String("req", req.RedactedString()),
	)
	defer func() { endSpan(err) }()

	// Encrypt the credential data
	pv, credentials, err := m.encryptCredentials(req.Credentials)
	if err != nil {
		return nil, err
	} else {
		req.Credentials = credentials
	}

	// Insert the credential record
	var result schema.Credential
	if err := m.PoolConn.Tx(ctx, func(conn pg.Conn) error {
		return conn.With("pv", pv).Insert(ctx, &result, req)
	}); err != nil {
		return nil, pg.NormalizeError(err)
	}

	// Return success
	return types.Ptr(result), nil
}
