package filer

import (
	"context"
	"encoding/json"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type CredentialProvider interface {
	// Expand expands the given value using the credential provider, substituting any
	// credentials $PLACEHOLDER or ${PLACEHOLDER} as needed.
	Expand(ctx context.Context, value string) (string, error)

	// Get returns the credential for the key, as JSON
	Get(ctx context.Context, key string) (json.RawMessage, error)

	// Set stores the credential for the key, which is JSON, replacing any
	// credential with the key
	Set(ctx context.Context, key string, value json.RawMessage) error
}
