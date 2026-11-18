//go:build !client && !indexer_memory && !indexer_pg

package main

import (
	"os"
	"path/filepath"

	// Packages
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	sqlite "github.com/mutablelogic/go-filer/filer/indexer/sqlite"
	server "github.com/mutablelogic/go-server"
)

// IndexerFlags are the flags of the store of the indexes of volumes, which
// is a SQLite database, of make INDEXER_BACKEND=sqlite, which is the default
type IndexerFlags struct {
	IndexerPath string `name:"indexer.path" env:"${ENV_NAME}_INDEXER_PATH" placeholder:"FILE" help:"SQLite database of the indexes of volumes, which is index.sqlite, in the command's folder of the user's cache, unless it's set."`
}

// indexerFile is the file of the indexes, in the command's folder of the
// user's cache, without --indexer.path
const indexerFile = "index.sqlite"

// store returns the store of the indexes of volumes
func (flags IndexerFlags) store(ctx server.Cmd) (indexer.Store, error) {
	path := flags.IndexerPath
	if path == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return nil, err
		}
		path = filepath.Join(cache, ctx.Name(), indexerFile)
	}
	return sqlite.New(path)
}
