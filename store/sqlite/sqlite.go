// Package sqlite is a store of volumes, and their indexes, in a SQLite
// database, which is created when it doesn't exist, or in memory, of
// sqlite.Memory, such as for tests
package sqlite

import (
	"context"
	"errors"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	store "github.com/mutablelogic/go-filer/store"
	schema "github.com/mutablelogic/go-filer/store/sqlite/schema"
	sqlite "github.com/mutablelogic/go-sqlite"
	ncruces "github.com/mutablelogic/go-sqlite/ncruces"
	_ "github.com/mutablelogic/go-sqlite/ncruces/fts5"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Store is a store of volumes, and their indexes, in a SQLite database
type Store struct {
	conn sqlite.PoolConn // The pool's connection, with the schema's queries
	opts store.Opts
}

var _ store.Store = (*Store)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns a store of the path of its database file, such as
// /var/lib/filer/index.db, or sqlite.Memory, which is created when it
// doesn't exist, with its schema. Its queries are traced with the options'
// tracer, and the changes to its volumes are published to their
// broadcaster, once they're committed.
func New(ctx context.Context, path string, opts ...store.Opt) (_ *Store, err error) {
	if strings.TrimSpace(path) == "" {
		return nil, gofiler.ErrBadParameter.With("the SQLite store needs its path")
	}
	o, err := store.Apply(opts...)
	if err != nil {
		return nil, err
	}

	// Trace opening it, and creating its schema
	ctx, end := otel.StartSpan(o.Tracer, ctx, "filer.store.sqlite.New", attribute.String("arg.path", path))
	defer func() { end(err) }()

	// Open the database, with the options
	dbopts := schema.Functions()
	if o.Tracer != nil {
		dbopts = append(dbopts, sqlite.WithTracer(o.Tracer))
	}
	pool, err := ncruces.Open(ctx, path, dbopts...)
	if err != nil {
		return nil, storeErr(err)
	}

	// Create its schema, and bind its queries
	queries, err := schema.NewQueries()
	if err != nil {
		return nil, errors.Join(err, pool.Close())
	} else if err := schema.Create(ctx, pool); err != nil {
		return nil, errors.Join(storeErr(err), pool.Close())
	}
	return &Store{conn: pool.WithQueries(queries).(sqlite.PoolConn), opts: o}, nil
}

// Close closes the database
func (self *Store) Close() error {
	return self.conn.Close()
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// span starts a span of a method of the store, such as filer.store.sqlite.CreateVolume,
// with its arguments, when there's a tracer, and returns the function which
// ends it, with the method's error. The spans of its queries are within it.
func (self *Store) span(ctx context.Context, method string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	return otel.StartSpan(self.opts.Tracer, ctx, "filer.store.sqlite."+method, attrs...)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// volumeErr returns ErrNotFound, of the volume, when there's no row, or the
// store's error
func volumeErr(err error, volume string) error {
	if errors.Is(err, sqlite.ErrNotFound) {
		return gofiler.ErrNotFound.Withf("volume %q", volume)
	}
	return storeErr(err)
}

// storeErr returns the error of the database as the filer's, of its kind,
// such as ErrNotFound, or ErrBadParameter, of a constraint
func storeErr(err error) error {
	var filerErr gofiler.Err
	switch {
	case err == nil:
		return nil
	case errors.As(err, &filerErr), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, sqlite.ErrNotFound):
		return gofiler.ErrNotFound.With(err.Error())
	case errors.Is(err, sqlite.ErrConflict):
		return gofiler.ErrConflict.With(err.Error())
	case errors.Is(err, sqlite.ErrBadParameter):
		return gofiler.ErrBadParameter.With(err.Error())
	case errors.Is(err, sqlite.ErrNotAvailable):
		return gofiler.ErrServiceUnavailable.With(err.Error())
	}
	return gofiler.ErrInternalServerError.With(err.Error())
}
