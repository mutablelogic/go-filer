// Package pg is an index store in a Postgres database, which isn't
// implemented yet (#72): each of its methods returns ErrNotImplemented
package pg

import (
	"context"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Store is an index in a Postgres database
type Store struct {
	url string
}

var _ indexer.Store = (*Store)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns an index store of the URL of its database, such as postgres://localhost/filer
func New(url string) (*Store, error) {
	if strings.TrimSpace(url) == "" {
		return nil, gofiler.ErrBadParameter.With("the Postgres index store needs its url")
	}
	return &Store{url: url}, nil
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
