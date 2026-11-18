//go:build !client && indexer_memory

package main

import (
	// Packages
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	memory "github.com/mutablelogic/go-filer/filer/indexer/memory"
	server "github.com/mutablelogic/go-server"
)

// IndexerFlags are the flags of the store of the indexes of volumes, which
// is in memory, of make INDEXER_BACKEND=memory, so a volume is scanned
// again when the server starts
type IndexerFlags struct{}

// store returns the store of the indexes of volumes
func (IndexerFlags) store(server.Cmd) (indexer.Store, error) {
	return memory.New(), nil
}
