//go:build !client && !indexer_memory && !indexer_pg

package main

import (
	"path/filepath"

	// Packages
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	server "github.com/mutablelogic/go-server"
)

// IndexerFlags are the flags of the store of the indexes of volumes, which
// is a SQLite database, of make INDEXER_BACKEND=sqlite, which is the
// default, index.sqlite in the command's folder of the user's data, beside
// its credentials, which has none
type IndexerFlags struct{}

// indexFile is the name of the index's database, in the command's folder of
// the user's data
const indexFile = "index.sqlite"

// store returns the store of the indexes of volumes, and the function
// which closes it. An index of an earlier version, <name>-index.db, in the
// user's cache folder, is moved to where it is now, when there's none there.
func (IndexerFlags) store(ctx server.Cmd, opts ...store.Opt) (store.Store, func(), error) {
	dir, err := dataDir(ctx)
	if err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, indexFile)
	if old, err := oldCacheDir(); err != nil {
		return nil, nil, err
	} else if err := moveFrom(filepath.Join(old, ctx.Name()+"-index.db"), path, "-wal", "-shm"); err != nil {
		return nil, nil, err
	}
	s, err := sqlite.New(ctx.Context(), path, opts...)
	if err != nil {
		return nil, nil, err
	}
	return s, func() { _ = s.Close() }, nil
}
