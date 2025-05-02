package schema

import (
	"context"

	// Packages
	"github.com/djthorpe/go-pg"
)

const (
	APIPrefix     = "/filer/v1"
	HeaderMetaKey = "X-Object-Meta"
	SchemaName    = "filer"
)

const (
	BucketListLimit = 100
	ObjectListLimit = 1000
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func Bootstrap(ctx context.Context, conn pg.Conn) error {
	// Create the schema
	if err := pg.SchemaCreate(ctx, conn, SchemaName); err != nil {
		return err
	}
	// Create types, tables, ...
	if err := bootstrapObject(ctx, conn); err != nil {
		return err
	}

	// Commit the transaction
	return nil

}
