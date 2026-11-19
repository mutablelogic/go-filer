// Package sqlite is a store of volumes, and their indexes, in a SQLite
// database, which is created when it doesn't exist, or in memory, of
// sqlite.Memory, such as for tests
package sqlite

import (
	"context"
	"errors"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/filer/schema"
	store "github.com/mutablelogic/go-filer/filer/store"
	schema "github.com/mutablelogic/go-filer/filer/store/sqlite/schema"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	sqlite "github.com/mutablelogic/go-sqlite"
	ncruces "github.com/mutablelogic/go-sqlite/ncruces"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Store is a store of volumes, and their indexes, in a SQLite database
type Store struct {
	conn sqlite.PoolConn // The pool's connection, with the schema's queries
	opts store.Opts
}

var _ store.Store = (*Store)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns a store of the path of its database file, such as
// /var/lib/filer/index.db, or sqlite.Memory, which is created when it
// doesn't exist, with its schema. Its queries are traced with the options'
// tracer, and the changes to its volumes are published to their
// broadcaster, once they're committed.
func New(ctx context.Context, path string, opts ...store.Opt) (_ *Store, err error) {
	if strings.TrimSpace(path) == "" {
		return nil, gofiler.ErrBadParameter.With("the SQLite store needs its path")
	}
	o, err := store.Apply(opts...)
	if err != nil {
		return nil, err
	}

	// Trace opening it, and creating its schema
	ctx, end := otel.StartSpan(o.Tracer, ctx, "filer.store.sqlite.New", attribute.String("arg.path", path))
	defer func() { end(err) }()

	// Open the database, with the options
	var dbopts []sqlite.Opt
	if o.Tracer != nil {
		dbopts = append(dbopts, sqlite.WithTracer(o.Tracer))
	}
	pool, err := ncruces.Open(ctx, path, dbopts...)
	if err != nil {
		return nil, storeErr(err)
	}

	// Create its schema, and bind its queries
	queries, err := schema.NewQueries()
	if err != nil {
		return nil, errors.Join(err, pool.Close())
	} else if err := schema.Create(ctx, pool); err != nil {
		return nil, errors.Join(storeErr(err), pool.Close())
	}
	return &Store{conn: pool.WithQueries(queries).(sqlite.PoolConn), opts: o}, nil
}

// Close closes the database
func (self *Store) Close() error {
	return self.conn.Close()
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - VOLUMES

// CreateVolume stores a new volume, and returns it, or returns ErrConflict
// when a volume of its name is stored
func (self *Store) CreateVolume(ctx context.Context, volume filer.VolumeMeta) (_ *filer.Volume, err error) {
	ctx, end := self.span(ctx, "CreateVolume", attribute.String("arg.volume", types.Stringify(volume)))
	defer func() { end(err) }()
	if err := store.ValidateVolume(volume); err != nil {
		return nil, err
	}
	var result schema.VolumeList
	if err := self.conn.Insert(ctx, &result, schema.VolumePut(volume)); errors.Is(err, sqlite.ErrConflict) {
		return nil, gofiler.ErrConflict.Withf("volume already exists: %q", volume.Name)
	} else if err != nil {
		return nil, storeErr(err)
	}
	self.opts.Publish(changes.ChangeCreate, result[0])
	return result[0], nil
}

// UpdateVolume replaces a stored volume's URL, and whether it's enabled,
// keeping its index, and returns it, or returns ErrNotFound when it isn't
// stored. It's only modified, and published, when they change.
func (self *Store) UpdateVolume(ctx context.Context, volume filer.VolumeMeta) (_ *filer.Volume, err error) {
	ctx, end := self.span(ctx, "UpdateVolume", attribute.String("arg.volume", types.Stringify(volume)))
	defer func() { end(err) }()
	if err := store.ValidateVolume(volume); err != nil {
		return nil, err
	}
	var result schema.VolumeList
	changed := true
	if err := self.conn.Tx(ctx, func(conn sqlite.Conn) error {
		err := conn.Update(ctx, &result, schema.VolumeName(volume.Name), schema.VolumePut(volume))
		if !errors.Is(err, sqlite.ErrNotFound) {
			return err
		}
		changed = false
		return conn.Get(ctx, &result, schema.VolumeName(volume.Name))
	}); err != nil {
		return nil, volumeErr(err, volume.Name)
	}
	if changed {
		self.opts.Publish(changes.ChangeUpdate, result[0])
	}
	return result[0], nil
}

// GetVolume returns a volume, or ErrNotFound
func (self *Store) GetVolume(ctx context.Context, name string) (_ *filer.Volume, err error) {
	ctx, end := self.span(ctx, "GetVolume", attribute.String("arg.name", name))
	defer func() { end(err) }()
	var result schema.VolumeList
	if err := self.conn.Get(ctx, &result, schema.VolumeName(name)); err != nil {
		return nil, volumeErr(err, name)
	}
	return result[0], nil
}

// ListVolumes returns the volumes, in the order of their names
func (self *Store) ListVolumes(ctx context.Context) (_ []*filer.Volume, err error) {
	ctx, end := self.span(ctx, "ListVolumes")
	defer func() { end(err) }()
	result := schema.VolumeList{}
	if err := self.conn.List(ctx, &result, schema.VolumeListRequest{}); err != nil {
		return nil, storeErr(err)
	}
	return result, nil
}

// DeleteVolume removes a volume, with its index, or returns ErrNotFound
func (self *Store) DeleteVolume(ctx context.Context, name string) (err error) {
	ctx, end := self.span(ctx, "DeleteVolume", attribute.String("arg.name", name))
	defer func() { end(err) }()
	var deleted schema.VolumeList
	if err := self.conn.Delete(ctx, &deleted, schema.VolumeName(name)); err != nil {
		return volumeErr(err, name)
	}
	self.opts.Publish(changes.ChangeDelete, deleted[0])
	return nil
}

// State returns the state of the volume's index, which is StatusNone when
// it's never been indexed, or the volume isn't stored
func (self *Store) State(ctx context.Context, volume string) (_ filer.IndexState, err error) {
	ctx, end := self.span(ctx, "State", attribute.String("arg.volume", volume))
	defer func() { end(err) }()
	result := schema.VolumeState{Name: volume}
	if err := self.conn.Get(ctx, &result, result); errors.Is(err, sqlite.ErrNotFound) {
		return filer.IndexState{}, nil
	} else if err != nil {
		return filer.IndexState{}, storeErr(err)
	}
	return result.IndexState, nil
}

// SetState stores the state of the volume's index, or returns ErrNotFound
// when the volume isn't stored
func (self *Store) SetState(ctx context.Context, volume string, state filer.IndexState) (err error) {
	ctx, end := self.span(ctx, "SetState", attribute.String("arg.volume", volume), attribute.String("arg.state", types.Stringify(state)))
	defer func() { end(err) }()
	var result schema.VolumeState
	if err := self.conn.Update(ctx, &result, schema.VolumeState{Name: volume}, schema.VolumeState{Name: volume, IndexState: state}); err != nil {
		return volumeErr(err, volume)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - OBJECTS

// Get returns the object of the volume at the path, with its metadata, or
// ErrNotFound
func (self *Store) Get(ctx context.Context, volume, path string) (_ *filer.Object, err error) {
	ctx, end := self.span(ctx, "Get", attribute.String("arg.volume", volume), attribute.String("arg.path", path))
	defer func() { end(err) }()
	var result schema.ObjectList
	if err := self.conn.Get(ctx, &result, schema.ObjectKey{Volume: volume, Path: path}); errors.Is(err, sqlite.ErrNotFound) {
		return nil, gofiler.ErrNotFound.Withf("%q isn't indexed in volume %q", path, volume)
	} else if err != nil {
		return nil, storeErr(err)
	}
	return result[0], nil
}

// Put stores the objects of the volume, replacing those at their paths, and
// their metadata, in one transaction, or returns ErrNotFound when the
// volume isn't stored
func (self *Store) Put(ctx context.Context, volume string, objects ...*filer.Object) (err error) {
	ctx, end := self.span(ctx, "Put", attribute.String("arg.volume", volume), attribute.Int("arg.objects", len(objects)))
	defer func() { end(err) }()
	return storeErr(self.conn.Bulk(ctx, func(conn sqlite.Conn) error {
		var exists schema.VolumeList
		if err := conn.Get(ctx, &exists, schema.VolumeName(volume)); err != nil {
			return volumeErr(err, volume)
		}
		for _, object := range objects {
			if err := conn.Insert(ctx, nil, schema.ObjectPut{Volume: volume, Object: object}); err != nil {
				return err
			}
		}
		return nil
	}))
}

// Delete removes the object of the volume at the path, and every object
// beneath it, with their metadata, which isn't an error when there's none
func (self *Store) Delete(ctx context.Context, volume, path string) (err error) {
	ctx, end := self.span(ctx, "Delete", attribute.String("arg.volume", volume), attribute.String("arg.path", path))
	defer func() { end(err) }()
	return storeErr(self.conn.Delete(ctx, nil, schema.ObjectKey{Volume: volume, Path: path}))
}

// List returns, in the order of their keys, up to the request's limit of
// the objects of a folder, or every object beneath it when it's recursive,
// after the key it starts after, without the folder
func (self *Store) List(ctx context.Context, volume string, req store.ListRequest) (_ []*filer.Object, err error) {
	ctx, end := self.span(ctx, "List", attribute.String("arg.volume", volume), attribute.String("arg.req", types.Stringify(req)))
	defer func() { end(err) }()
	result := schema.ObjectList{}
	if err := self.conn.List(ctx, &result, schema.ObjectListRequest{Volume: volume, ListRequest: req}); err != nil {
		return nil, storeErr(err)
	}
	return result, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// span starts a span of a method of the store, such as filer.store.sqlite.CreateVolume,
// with its arguments, when there's a tracer, and returns the function which
// ends it, with the method's error. The spans of its queries are within it.
func (self *Store) span(ctx context.Context, method string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	return otel.StartSpan(self.opts.Tracer, ctx, "filer.store.sqlite."+method, attrs...)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// volumeErr returns ErrNotFound, of the volume, when there's no row, or the
// store's error
func volumeErr(err error, volume string) error {
	if errors.Is(err, sqlite.ErrNotFound) {
		return gofiler.ErrNotFound.Withf("volume %q", volume)
	}
	return storeErr(err)
}

// storeErr returns the error of the database as the filer's, of its kind,
// such as ErrNotFound, or ErrBadParameter, of a constraint
func storeErr(err error) error {
	var filerErr gofiler.Err
	switch {
	case err == nil:
		return nil
	case errors.As(err, &filerErr), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, sqlite.ErrNotFound):
		return gofiler.ErrNotFound.With(err.Error())
	case errors.Is(err, sqlite.ErrConflict):
		return gofiler.ErrConflict.With(err.Error())
	case errors.Is(err, sqlite.ErrBadParameter):
		return gofiler.ErrBadParameter.With(err.Error())
	case errors.Is(err, sqlite.ErrNotAvailable):
		return gofiler.ErrServiceUnavailable.With(err.Error())
	}
	return gofiler.ErrInternalServerError.With(err.Error())
}
