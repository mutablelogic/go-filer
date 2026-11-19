//go:build !client && indexer_memory

package main

import (
	// Packages
	store "github.com/mutablelogic/go-filer/filer/store"
	sqlite "github.com/mutablelogic/go-filer/filer/store/sqlite"
	server "github.com/mutablelogic/go-server"
	gosqlite "github.com/mutablelogic/go-sqlite"
)

// IndexerFlags are the flags of the store of volumes, and their indexes,
// which is a SQLite database in memory, of make INDEXER_BACKEND=memory, so
// volumes aren't kept, and are scanned again, when the server starts
type IndexerFlags struct{}

// store returns the store of the indexes of volumes, and the function
// which closes it, when the server stops
func (IndexerFlags) store(ctx server.Cmd, opts ...store.Opt) (store.Store, func(), error) {
	s, err := sqlite.New(ctx.Context(), gosqlite.Memory, opts...)
	if err != nil {
		return nil, nil, err
	}
	return s, func() { _ = s.Close() }, nil
}
