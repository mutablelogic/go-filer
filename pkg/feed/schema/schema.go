package schema

import (
	"context"
	"time"

	// Packages
	pg "github.com/djthorpe/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	SchemaName     = "feed"
	ItemListLimit  = 1000
	APIPrefix      = "/feed/v1"
	DefaultFeedTTL = time.Hour * 6
)

var (
	acceptableSchemes = []string{types.SchemeInsecure, types.SchemeSecure}
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func Bootstrap(ctx context.Context, conn pg.Conn) error {
	// Create the schema
	if err := pg.SchemaCreate(ctx, conn, SchemaName); err != nil {
		return err
	}

	// Create types, tables, ...
	if err := bootstrapUrl(ctx, conn); err != nil {
		return err
	}
	if err := bootstrapFeed(ctx, conn); err != nil {
		return err
	}

	// Commit the transaction
	return nil

}
