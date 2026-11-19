// Package schema is the schema of the SQLite store: its tables, its
// queries, and the types which are read from, and written to, them
package schema

import (
	"context"
	_ "embed"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	sqlite "github.com/mutablelogic/go-sqlite"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// Version is the version of the schema, of the database's user_version
const Version = 4

var (
	//go:embed objects.sql
	objects string

	//go:embed queries.sql
	queries string
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Create creates the schema, in a transaction, when the database hasn't
// one, and sets its version. A database whose schema is newer is an error.
func Create(ctx context.Context, conn sqlite.Conn) error {
	objects, err := sqlite.NewQueries(strings.NewReader(objects))
	if err != nil {
		return err
	}
	return conn.Tx(ctx, func(conn sqlite.Conn) error {
		version, err := sqlite.Version(ctx, conn)
		if err != nil {
			return err
		} else if version > Version {
			return gofiler.ErrNotImplemented.Withf("the store's schema version %d is newer than %d", version, Version)
		} else if version == Version {
			return nil
		}

		// Version 4's volumes have how many files the last scan indexed,
		// which a volume of version 1 to 3 is added, as NULL
		if version >= 1 && version < 4 {
			if err := conn.With(sqlite.TraceSpanNameArg, "schema.migrate_4").Exec(ctx, `ALTER TABLE volume ADD COLUMN files INTEGER`); err != nil {
				return err
			}
		}
		for _, key := range objects.Keys() {
			if err := conn.With(sqlite.TraceSpanNameArg, key).Exec(ctx, objects.Query(key)); err != nil {
				return err
			}
		}
		return sqlite.SetVersion(ctx, conn, Version)
	})
}

// NewQueries returns the queries, which a connection is bound to, with
// WithQueries, so the types' selectors and writers use them
func NewQueries() (*sqlite.Queries, error) {
	return sqlite.NewQueries(strings.NewReader(queries))
}
