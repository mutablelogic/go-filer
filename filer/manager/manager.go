package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	backendregistry "github.com/mutablelogic/go-filer/backend/registry"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadatamanager "github.com/mutablelogic/go-filer/metadata/manager"
	llm "github.com/mutablelogic/go-llm/provider/registry"
	pg "github.com/mutablelogic/go-pg"
	pgqueue "github.com/mutablelogic/go-pg/pgqueue/manager"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opt
	pg.PoolConn
	volumes  *backendregistry.Registry
	queue    *pgqueue.Manager
	metadata *metadatamanager.Manager
	llm      *llm.Registry
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new manager object
func New(ctx context.Context, pool pg.PoolConn, opts ...Opt) (_ *Manager, err error) {
	self := new(Manager)

	// Apply options
	if err := self.opt.apply(opts); err != nil {
		return nil, err
	} else if queue, err := pgqueue.New(ctx, pool, pgqueue.WithSchema(self.schema), pgqueue.WithMeter(self.metrics), pgqueue.WithTracer(self.tracer)); err != nil {
		return nil, err
	} else if metadata, err := metadatamanager.New(ctx, metadatamanager.WithMeter(self.metrics), metadatamanager.WithTracer(self.tracer)); err != nil {
		return nil, err
	} else if registry := llm.New(self.opt.clientopts...); registry == nil {
		return nil, fmt.Errorf("failed to create llm registry")
	} else {
		self.volumes = backendregistry.New()
		self.queue = queue
		self.metadata = metadata
		self.llm = registry
	}

	// Parse and register named queries so bind.Query(...) can resolve them.
	queries, err := pg.NewQueries(strings.NewReader(schema.Queries))
	if err != nil {
		return nil, fmt.Errorf("parse queries.sql: %w", err)
	} else if pool == nil {
		return nil, gofiler.ErrBadParameter.With("pg pool is required")
	} else {
		pool = pool.WithQueries(queries).With(
			"schema", self.schema,
			"notify_channel", schema.NotifyChannel,
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
		self.PoolConn = pool
	}

	// Register metrics
	if self.metrics != nil {
		err := errors.Join(
			self.RegisterVolumeMetrics("filer_volume"),
		)
		if err != nil {
			return nil, fmt.Errorf("register metrics: %w", err)
		}
	}

	// Return success
	endBootstrapSpan(nil)
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
