package manager

import (
	"context"
	"net/http"

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

// RotateCredential re-encrypts a credential with the latest passphrase version.
// Returns gofiler.ErrNotModified if the credential is already at the latest version.
func (m *Manager) RotateCredential(ctx context.Context, key schema.CredentialKey) (_ *schema.Credential, err error) {
	ctx, endSpan := otel.StartSpan(m.tracer, ctx, "RotateCredential",
		attribute.String("key", key.Key),
	)
	defer func() { endSpan(err) }()

	// Fetch current row to get the stored PV and ciphertext.
	var current schema.CredentialGet
	if err := m.PoolConn.Get(ctx, &current, key); err != nil {
		return nil, pg.NormalizeError(err)
	}

	// Determine the latest passphrase version.
	_, latestPV := m.passphrases.Get(0)
	if latestPV == 0 {
		return nil, httpresponse.ErrServiceUnavailable.Withf("no encryption passphrase configured for credentials")
	}

	// Nothing to do if the credential is already at the latest version.
	if current.PV == latestPV {
		return nil, httpresponse.Err(http.StatusNotModified)
	}

	// Decrypt with the current passphrase version.
	encrypted, ok := current.Credentials.([]byte)
	if !ok {
		return nil, httpresponse.ErrInternalError.With("credential payload is invalid")
	}
	plaintext, err := m.passphrases.Decrypt(current.PV, string(encrypted))
	if err != nil {
		return nil, httpresponse.ErrBadRequest.With(err)
	}

	// Re-encrypt with the latest passphrase version.
	newPV, newCiphertext, err := m.passphrases.Encrypt(0, plaintext)
	if err != nil {
		return nil, httpresponse.ErrBadRequest.With(err)
	}

	// Upsert the credential with the new PV and ciphertext.
	var result schema.Credential
	if err := m.PoolConn.Tx(ctx, func(conn pg.Conn) error {
		return conn.With("pv", newPV).Insert(ctx, &result, schema.CredentialCreate{
			CredentialKey: key,
			Credentials:   []byte(newCiphertext),
		})
	}); err != nil {
		return nil, pg.NormalizeError(err)
	}

	return types.Ptr(result), nil
}
