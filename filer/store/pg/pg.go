// Package pg is an index store in a Postgres database, which isn't
// implemented yet (#72): each of its methods returns ErrNotImplemented
package pg

import (
	"context"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	store "github.com/mutablelogic/go-filer/filer/store"
	pg "github.com/mutablelogic/go-pg"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Store is an index in a Postgres database, of a pool of its connections
type Store struct {
	conn pg.PoolConn
	opts store.Opts
}

var _ store.Store = (*Store)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns an index store of a pool of connections to its database,
// such as go-pg's pkg/cmd PostgresFlags' Connect returns, which its owner
// closes
func New(conn pg.PoolConn, opts ...store.Opt) (*Store, error) {
	if conn == nil {
		return nil, gofiler.ErrBadParameter.With("the Postgres index store needs a connection to its database")
	}
	o, err := store.Apply(opts...)
	if err != nil {
		return nil, err
	}
	return &Store{conn: conn, opts: o}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (self *Store) CreateVolume(context.Context, schema.VolumeMeta) (*schema.Volume, error) {
	return nil, errNotImplemented
}

func (self *Store) UpdateVolume(context.Context, schema.VolumeMeta) (*schema.Volume, error) {
	return nil, errNotImplemented
}

func (self *Store) GetVolume(context.Context, string) (*schema.Volume, error) {
	return nil, errNotImplemented
}

func (self *Store) ListVolumes(context.Context) ([]*schema.Volume, error) {
	return nil, errNotImplemented
}

func (self *Store) DeleteVolume(context.Context, string) error {
	return errNotImplemented
}

func (self *Store) Get(context.Context, string, string) (*schema.Object, error) {
	return nil, errNotImplemented
}

func (self *Store) Put(context.Context, string, ...*schema.Object) error {
	return errNotImplemented
}

func (self *Store) Delete(context.Context, string, string) error {
	return errNotImplemented
}

func (self *Store) List(context.Context, string, store.ListRequest) ([]*schema.Object, error) {
	return nil, errNotImplemented
}

func (self *Store) State(context.Context, string) (schema.IndexState, error) {
	return schema.IndexState{}, errNotImplemented
}

func (self *Store) SetState(context.Context, string, schema.IndexState) error {
	return errNotImplemented
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// errNotImplemented is the error of each of the store's methods, until it's
// implemented
var errNotImplemented = gofiler.ErrNotImplemented.With("the Postgres index store isn't implemented yet")
