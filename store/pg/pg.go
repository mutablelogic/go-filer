// Package pg is an index store in a Postgres database, which isn't
// implemented yet (#72): each of its methods returns ErrNotImplemented
package pg

import (
	"context"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
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

func (self *Store) GetArtwork(context.Context, string, bool) (*schema.Artwork, error) {
	return nil, errNotImplemented
}

func (self *Store) SetObjectArtwork(context.Context, string, string, ...schema.ArtworkMeta) ([]*schema.Artwork, error) {
	return nil, errNotImplemented
}

func (self *Store) ObjectArtwork(context.Context, string, string) ([]*schema.Artwork, error) {
	return nil, errNotImplemented
}

func (self *Store) DeleteUnlinkedArtwork(context.Context) (int, error) {
	return 0, errNotImplemented
}

func (self *Store) Push(context.Context, string, ...store.Work) error {
	return errNotImplemented
}

func (self *Store) Claim(context.Context, string, []store.Kind, int, time.Duration) ([]store.Work, error) {
	return nil, errNotImplemented
}

func (self *Store) Done(context.Context, string, ...store.Work) error {
	return errNotImplemented
}

func (self *Store) Failed(context.Context, string, store.Work, error, time.Duration) error {
	return errNotImplemented
}

func (self *Store) PushStale(context.Context, string, string, store.Kind) error {
	return errNotImplemented
}

func (self *Store) Search(context.Context, schema.SearchRequest) (*schema.SearchResponse, error) {
	return nil, errNotImplemented
}

func (self *Store) Tasks(context.Context, string, store.Kind) (schema.TaskState, error) {
	return schema.TaskState{}, errNotImplemented
}

func (self *Store) Pending(context.Context, string, string) (bool, error) {
	return false, errNotImplemented
}

func (self *Store) PendingBeneath(context.Context, string, string) (bool, error) {
	return false, errNotImplemented
}

func (self *Store) CountObjects(context.Context, string, store.ListRequest) (uint64, error) {
	return 0, errNotImplemented
}

func (self *Store) Kinds(context.Context, string) (map[string]uint64, error) {
	return nil, errNotImplemented
}

func (self *Store) Seen(context.Context, string, map[string]string) error {
	return errNotImplemented
}

func (self *Store) Unsee(context.Context, string, string) error {
	return errNotImplemented
}

func (self *Store) Sweep(context.Context, string, string) error {
	return errNotImplemented
}

func (self *Store) Changed(context.Context, string, string, string, int) ([]store.Seen, error) {
	return nil, errNotImplemented
}

func (self *Store) CountChanged(context.Context, string, string) (uint64, error) {
	return 0, errNotImplemented
}

func (self *Store) Revisions(context.Context, string, string, bool) (map[string]string, error) {
	return nil, errNotImplemented
}

func (self *Store) SetRevisions(context.Context, string, map[string]string) error {
	return errNotImplemented
}

func (self *Store) GetObject(context.Context, string, string) (*schema.Object, error) {
	return nil, errNotImplemented
}

func (self *Store) PutObject(context.Context, string, ...*schema.Object) error {
	return errNotImplemented
}

func (self *Store) SetMetadata(context.Context, string, string, string, *schema.ObjectAttr, schema.TaskMeta) error {
	return errNotImplemented
}

func (self *Store) DeleteMetadata(context.Context, string, string) error {
	return errNotImplemented
}

func (self *Store) DeleteObject(context.Context, string, string) error {
	return errNotImplemented
}

func (self *Store) ListObjects(context.Context, string, store.ListRequest) ([]*schema.Object, error) {
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
