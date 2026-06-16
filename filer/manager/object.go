package manager

import (
	"context"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) createObject(ctx context.Context, req schema.ObjectCreate) (_ *schema.Object, err error) {
	// Cannot create directories
	if req.IsDir {
		return nil, gofiler.ErrConflict.With("cannot create directory object")
	}

	// Upsert the object
	var result schema.Object
	if err := manager.Tx(ctx, func(conn pg.Conn) error {
		// TODO: delete
		return conn.Insert(ctx, &result, req)
		// TODO: upsert the metadata
	}); err != nil {
		return nil, err
	}

	// Return success
	return types.Ptr(result), nil
}
