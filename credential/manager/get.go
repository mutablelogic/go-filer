package manager

import (
	"context"
	"encoding/json"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/credential/schema"
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// GetCredential retrieves a credential by key and decrypts the credential payload with
// the given passphrase
func (m *Manager) GetCredential(ctx context.Context, key schema.CredentialKey, passphrase string) (_ json.RawMessage, err error) {
	ctx, endSpan := otel.StartSpan(m.tracer, ctx, "GetCredential",
		attribute.String("key", key.Key),
	)
	defer func() { endSpan(err) }()

	// Fetch the credential by key to get the stored passphrase version.
	var result schema.CredentialGet
	if err := m.PoolConn.Get(ctx, &result, key); err != nil {
		return nil, pg.NormalizeError(err)
	}

	// Verify the provided passphrase matches the stored passphrase version.
	stored, _ := m.passphrases.Get(result.PV)
	if stored != passphrase {
		return nil, httpresponse.ErrBadRequest.Withf("invalid passphrase")
	}

	// Decrypt the credential.
	encrypted, ok := result.Credentials.([]byte)
	if !ok {
		return nil, httpresponse.ErrInternalError.With("credential payload is invalid")
	}
	var credentials json.RawMessage
	if err := m.decryptCredentials(encrypted, result.PV, &credentials); err != nil {
		return nil, err
	}

	// Return success
	return credentials, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - INTERNAL USE ONLY

// GetCredentialWithoutPassphrase retrieves and decrypts a credential using the stored
// passphrase version. It is intended for internal server-side use only and must not
// be exposed via any public API or HTTP handler.
func (m *Manager) GetCredentialWithoutPassphrase(ctx context.Context, key schema.CredentialKey) (_ json.RawMessage, err error) {
	ctx, endSpan := otel.StartSpan(m.tracer, ctx, "GetCredentialWithoutPassphrase",
		attribute.String("key", key.Key),
	)
	defer func() { endSpan(err) }()

	var result schema.CredentialGet
	var credentials json.RawMessage
	if err := m.PoolConn.Get(ctx, &result, key); err != nil {
		return nil, pg.NormalizeError(err)
	} else if encrypted, ok := result.Credentials.([]byte); !ok {
		return nil, httpresponse.ErrInternalError.With("credential payload is invalid")
	} else if err := m.decryptCredentials(encrypted, result.PV, &credentials); err != nil {
		return nil, err
	}

	// Return the decrypted JSON payload
	return credentials, nil
}
