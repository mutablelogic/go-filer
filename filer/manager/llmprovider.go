package manager

import (
	"context"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	llmschema "github.com/mutablelogic/go-llm/kernel/schema"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// CreateLLMProvider creates a new LLM provider which can be used for extracting
// metadata whilst indexing objects
func (m *Manager) CreateLLMProvider(ctx context.Context, req schema.LLMProviderCreate) (_ *schema.LLMProvider, err error) {
	ctx, endSpan := otel.StartSpan(m.tracer, ctx, "CreateLLMProvider",
		attribute.String("req", req.String()),
	)
	defer func() { endSpan(err) }()

	// Validate the provider by attempting to create a client and ping it.
	var provider llmschema.Provider
	var credentials llmschema.ProviderCredentials
	provider.Provider = req.Provider
	provider.URL = req.URL

	// TODO: Fetch credentials
	if err := m.llm.Validate(ctx, provider, credentials); err != nil {
		return nil, err
	}

	// Insert the provider into the database
	var result schema.LLMProvider
	if err := m.PoolConn.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.Insert(ctx, &result, req); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, pg.NormalizeError(err)
	}

	// Return success
	return types.Ptr(result), nil
}
