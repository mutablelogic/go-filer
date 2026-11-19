//go:build !client && !indexer_memory && !indexer_pg

package main

import (
	"os"
	"path/filepath"

	// Packages
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	server "github.com/mutablelogic/go-server"
)

// IndexerFlags are the flags of the store of the indexes of volumes, which
// is a SQLite database, of make INDEXER_BACKEND=sqlite, which is the
// default, <name>-index.db in the user's cache folder, which has none
type IndexerFlags struct{}

// store returns the store of the indexes of volumes, and the function
// which closes it, when the server stops
func (IndexerFlags) store(ctx server.Cmd, opts ...store.Opt) (store.Store, func(), error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return nil, nil, err
	}
	s, err := sqlite.New(ctx.Context(), filepath.Join(cache, ctx.Name()+"-index.db"), opts...)
	if err != nil {
		return nil, nil, err
	}
	return s, func() { _ = s.Close() }, nil
}
