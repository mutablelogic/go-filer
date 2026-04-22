package manager

import (
	"context"
	"fmt"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/queue/schema"
	pg "github.com/mutablelogic/go-pg"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opt
	pg.PoolConn
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(ctx context.Context, name, version string, pool pg.PoolConn, opts ...Opt) (*Manager, error) {
	// Set default values
	self := new(Manager)
	self.defaults(name, version)

	// Check arguments
	if pool == nil {
		return nil, fmt.Errorf("pool is required")
	}

	// Apply options
	if err := self.apply(opts...); err != nil {
		return nil, err
	}

	// Parse and register named queries so bind.Query(...) can resolve them.
	queries, err := pg.NewQueries(strings.NewReader(schema.Queries))
	if err != nil {
		return nil, fmt.Errorf("parse queries.sql: %w", err)
	} else {
		pool = pool.WithQueries(queries).With(
			"schema", self.schema,
		).(pg.PoolConn)
	}

	// Create objects in the database schema. This is not done in a transaction
	bootstrapCtx, endBootstrapSpan := otel.StartSpan(self.tracer, ctx, "bootstrap",
		attribute.String("schema", self.schema),
	)
	if err := bootstrap(bootstrapCtx, pool, self.schema); err != nil {
		endBootstrapSpan(err)
		return nil, err
	} else {
		endBootstrapSpan(nil)
		self.PoolConn = pool
	}

	// Return success
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func bootstrap(ctx context.Context, conn pg.Conn, schemaName string) error {
	// Get all objects
	objects, err := pg.NewQueries(strings.NewReader(schema.Objects))
	if err != nil {
		return fmt.Errorf("parse objects.sql: %w", err)
	}

	// Create the schema
	if err := pg.SchemaCreate(ctx, conn, schemaName); err != nil {
		return fmt.Errorf("create schema %q: %w", schemaName, err)
	}

	// Create all objects - not in a transaction
	for _, key := range objects.Keys() {
		if err := conn.Exec(ctx, objects.Query(key)); err != nil {
			return fmt.Errorf("create object %q: %w", key, err)
		}
	}

	// Return success
	return nil
}
