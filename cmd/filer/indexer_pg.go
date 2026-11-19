//go:build !client && indexer_pg

package main

import (
	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	store "github.com/mutablelogic/go-filer/store"
	pg "github.com/mutablelogic/go-filer/store/pg"
	pgcmd "github.com/mutablelogic/go-pg/pkg/cmd"
	server "github.com/mutablelogic/go-server"
)

// IndexerFlags are the flags of the store of the indexes of volumes, which
// is a Postgres database, of make INDEXER_BACKEND=pg, of go-pg's flags,
// --pg.url and --pg.password
type IndexerFlags struct {
	Postgres pgcmd.PostgresFlags `embed:"" prefix:"pg."`
}

// store returns the store of the indexes of volumes, of a pool of
// connections to its database, and the function which closes it, when the
// server stops
func (flags IndexerFlags) store(ctx server.Cmd, opts ...store.Opt) (store.Store, func(), error) {
	conn, err := flags.Postgres.Connect(ctx)
	if err != nil {
		return nil, nil, err
	} else if conn == nil {
		return nil, nil, gofiler.ErrBadParameter.With("the Postgres index store needs its database, with --pg.url, or PG_URL")
	}
	s, err := pg.New(conn, opts...)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return s, conn.Close, nil
}
