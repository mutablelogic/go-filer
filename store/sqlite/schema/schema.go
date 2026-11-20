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
const Version = 16

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

		// Version 5's volumes have what their last scan indexed, which a
		// volume of version 1 to 4 is added, as NULL, so it's scanned again
		// before its files are listed from the index
		if version >= 1 && version < 5 {
			if err := conn.With(sqlite.TraceSpanNameArg, "schema.migrate_5").Exec(ctx, `ALTER TABLE volume ADD COLUMN scan_mode TEXT`); err != nil {
				return err
			}
		}
		// Version 6's objects, their metadata, and the queue, are of keys,
		// rather than paths, so the index of a volume of version 1 to 5 is
		// removed, and its state, so it's scanned again, as is its work
		if version >= 1 && version < 6 {
			for _, query := range []string{
				`DROP TABLE IF EXISTS metadata`,
				`DROP TABLE IF EXISTS object_state`,
				`DROP TABLE IF EXISTS object`,
				`UPDATE volume SET status = NULL, folders = NULL, files = NULL, scan_mode = NULL, index_started = NULL, index_timestamp = NULL, error = NULL`,
			} {
				if err := conn.With(sqlite.TraceSpanNameArg, "schema.migrate_6").Exec(ctx, query); err != nil {
					return err
				}
			}
		}
		// Version 7's queue has the revisions of the files which are listed,
		// which a queue of version 6 is added, as NULL, so its files are got
		// again, at their next scan, and a queue before it is created with
		if version == 6 {
			if err := conn.With(sqlite.TraceSpanNameArg, "schema.migrate_7").Exec(ctx, `ALTER TABLE object_state ADD COLUMN revision TEXT`); err != nil {
				return err
			}
		}
		// Version 8's objects have their media type, of a listing of a type,
		// which an object of version 6 or 7 is added, as it's generated
		if version == 6 || version == 7 {
			if err := conn.With(sqlite.TraceSpanNameArg, "schema.migrate_8").Exec(ctx, `ALTER TABLE object ADD COLUMN media_type TEXT GENERATED ALWAYS AS (
				CASE WHEN type IS NULL AND key GLOB '*/' THEN 'inode/directory'
				ELSE lower(trim(CASE WHEN instr(type, ';') > 0 THEN substr(type, 1, instr(type, ';') - 1) ELSE type END))
				END) VIRTUAL`); err != nil {
				return err
			}
		}
		// Version 9's objects have their subtypes, of a listing of a
		// subtype, which an object of version 6 to 8 is added, as it's
		// generated
		if version >= 6 && version < 9 {
			if err := conn.With(sqlite.TraceSpanNameArg, "schema.migrate_9").Exec(ctx, `ALTER TABLE object ADD COLUMN media_subtype TEXT GENERATED ALWAYS AS (
				substr(media_type, instr(media_type, '/') + 1)) VIRTUAL`); err != nil {
				return err
			}
		}
		// Version 10's volumes have what their scan in progress is doing,
		// which a volume of version 1 to 9 is added, as NULL, so it's idle
		if version >= 1 && version < 10 {
			if err := conn.With(sqlite.TraceSpanNameArg, "schema.migrate_10").Exec(ctx, `ALTER TABLE volume ADD COLUMN phase TEXT`); err != nil {
				return err
			}
		}
		// Version 11's metadata is of its source, the backend's, or a task's,
		// which metadata of version 6 to 10 is added, as the backend's, and
		// its index is created again, of its source, as is the trigger of an
		// object which is deleted, which removes the state of its tasks
		if version >= 6 && version < 11 {
			for _, query := range []string{
				`ALTER TABLE metadata ADD COLUMN source TEXT NOT NULL DEFAULT ''`,
				`DROP INDEX IF EXISTS metadata_key`,
				`DROP TRIGGER IF EXISTS object_deleted`,
			} {
				if err := conn.With(sqlite.TraceSpanNameArg, "schema.migrate_11").Exec(ctx, query); err != nil {
					return err
				}
			}
		}
		// Version 12's queue has when work last failed, of its error, apart
		// from when it's due again, which a queue of version 6 to 11 is
		// added, as NULL, and a queue before it is created with
		if version >= 6 && version < 12 {
			if err := conn.With(sqlite.TraceSpanNameArg, "schema.migrate_12").Exec(ctx, `ALTER TABLE object_state ADD COLUMN failed TIMESTAMP`); err != nil {
				return err
			}
		}
		// Version 16's queue counts the failures of work whose backend was
		// unavailable apart from its attempts, which a queue of version 6 to
		// 15 is added, as none, and a queue before it is created with
		if version >= 6 && version < 16 {
			if err := conn.With(sqlite.TraceSpanNameArg, "schema.migrate_16").Exec(ctx, `ALTER TABLE object_state ADD COLUMN outages INTEGER NOT NULL DEFAULT 0`); err != nil {
				return err
			}
		}
		// Version 13 has the shapes of where objects are, and 14 the times of
		// their events, whose tables are created, as a schema's of every
		// version is
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
