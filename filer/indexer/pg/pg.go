// Package pg is an index store in a Postgres database, which isn't
// implemented yet (#72): each of its methods returns ErrNotImplemented
package pg

import (
	"context"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	pg "github.com/mutablelogic/go-pg"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Store is an index in a Postgres database, of a pool of its connections
type Store struct {
	conn pg.PoolConn
}

var _ indexer.Store = (*Store)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns an index store of a pool of connections to its database,
// such as go-pg's pkg/cmd PostgresFlags' Connect returns, which its owner
// closes
func New(conn pg.PoolConn) (*Store, error) {
	if conn == nil {
		return nil, gofiler.ErrBadParameter.With("the Postgres index store needs a connection to its database")
	}
	return &Store{conn: conn}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (self *Store) Get(context.Context, string, string) (*schema.Object, error) {
	return nil, errNotImplemented
}

func (self *Store) Put(context.Context, string, ...*schema.Object) error {
	return errNotImplemented
}

func (self *Store) Delete(context.Context, string, string) error {
	return errNotImplemented
}

func (self *Store) List(context.Context, string, indexer.ListRequest) ([]*schema.Object, error) {
	return nil, errNotImplemented
}

func (self *Store) State(context.Context, string) (indexer.State, error) {
	return indexer.State{}, errNotImplemented
}

func (self *Store) SetState(context.Context, string, indexer.State) error {
	return errNotImplemented
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// errNotImplemented is the error of each of the store's methods, until it's
// implemented
var errNotImplemented = gofiler.ErrNotImplemented.With("the Postgres index store isn't implemented yet")
