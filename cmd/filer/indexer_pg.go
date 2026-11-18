//go:build !client && indexer_pg

package main

import (
	// Packages
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	pg "github.com/mutablelogic/go-filer/filer/indexer/pg"
	server "github.com/mutablelogic/go-server"
)

// IndexerFlags are the flags of the store of the indexes of volumes, which
// is a Postgres database, of make INDEXER_BACKEND=pg
type IndexerFlags struct {
	IndexerURL string `name:"indexer.url" env:"${ENV_NAME}_INDEXER_URL" required:"" placeholder:"URL" help:"Postgres database of the indexes of volumes, such as postgres://localhost/filer."`
}

// store returns the store of the indexes of volumes
func (flags IndexerFlags) store(server.Cmd) (indexer.Store, error) {
	return pg.New(flags.IndexerURL)
}
