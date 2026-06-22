package manager

import (
	"context"
	"encoding/json"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
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

// GetCredential retrieves a credential by key and decrypts the credential payload with
// the given passphrase
func (m *Manager) GetCredential(ctx context.Context, key schema.CredentialKey, passphrase string) (_ json.RawMessage, err error) {
	ctx, endSpan := otel.StartSpan(m.tracer, ctx, "GetCredential",
		attribute.String("key", key.Key),
	)
	defer func() { endSpan(err) }()

	// Determine the passphrase version for the provided passphrase.
	var pv uint64
	keys := m.passphrases.Keys()
	for i := len(keys) - 1; i >= 0; i-- {
		resolved, version := m.passphrases.Get(keys[i])
		if resolved == passphrase {
			pv = version
			break
		}
	}
	if pv == 0 {
		return nil, gofiler.ErrBadParameter.Withf("invalid passphrase")
	}

	var result schema.CredentialCreate
	var credentials json.RawMessage
	if err := m.PoolConn.With("pv", pv).Get(ctx, &result, key); err != nil {
		return nil, pg.NormalizeError(err)
	} else if encrypted, ok := result.Credentials.([]byte); !ok {
		return nil, gofiler.ErrInternalServerError.With("credential payload is invalid")
	} else if err := m.decryptCredentials(encrypted, pv, &credentials); err != nil {
		return nil, err
	}

	// Return success
	return credentials, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (m *Manager) encryptCredentials(v any) (uint64, []byte, error) {
	// Preserve the zero-value contract for raw credential payloads.
	switch value := v.(type) {
	case nil:
		return 0, []byte{}, nil
	case []byte:
		if len(value) == 0 {
			return 0, []byte{}, nil
		}
	case string:
		if value == "" {
			return 0, []byte{}, nil
		}
	}

	// Turn the credentials into JSON. If the credentials are empty this will
	// return an empty JSON object, which we can treat as an empty byte array.
	data, err := json.Marshal(v)
	if err != nil {
		return 0, nil, gofiler.ErrBadParameter.With(err)
	} else if string(data) == "{}" {
		return 0, []byte{}, nil
	}

	// Check for at least one passphrase configured
	if len(m.passphrases.Keys()) == 0 {
		return 0, nil, gofiler.ErrServiceUnavailable.Withf("no encryption passphrase configured for credentials")
	}

	// Get the encryption passphrase for the current passphrase version. If there is no
	// passphrase configured for the current version, return an error
	if pv, crypted, err := m.passphrases.Encrypt(0, data); err != nil {
		return 0, nil, gofiler.ErrBadParameter.With(err)
	} else {
		return pv, []byte(crypted), nil
	}
}

func (m *Manager) decryptCredentials(encrypted []byte, pv uint64, decrypted any) error {
	if len(encrypted) == 0 {
		return nil
	}

	// Check for at least one passphrase configured
	if len(m.passphrases.Keys()) == 0 {
		return gofiler.ErrServiceUnavailable.Withf("no encryption passphrase configured for credentials")
	}

	// Decrypt the credentials using the passphrase version and encrypted data, and
	// then unmarshal the JSON into the provided decrypted structure.
	if data, err := m.passphrases.Decrypt(pv, string(encrypted)); err != nil {
		return gofiler.ErrBadParameter.With(err)
	} else if err := json.Unmarshal([]byte(data), decrypted); err != nil {
		return gofiler.ErrBadParameter.With(err)
	} else {
		return nil
	}
}
