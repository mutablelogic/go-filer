//go:build !client && indexer_memory

package main

import (
	// Packages
	store "github.com/mutablelogic/go-filer/filer/store"
	memory "github.com/mutablelogic/go-filer/filer/store/memory"
	server "github.com/mutablelogic/go-server"
)

// IndexerFlags are the flags of the store of the indexes of volumes, which
// is in memory, of make INDEXER_BACKEND=memory, so a volume is scanned
// again when the server starts
type IndexerFlags struct{}

// store returns the store of the indexes of volumes, and the function
// which closes it, when the server stops
func (IndexerFlags) store(_ server.Cmd, opts ...store.Opt) (store.Store, func(), error) {
	s, err := memory.New(opts...)
	return s, func() {}, err
}
