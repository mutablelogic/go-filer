package sqlite

import (
	"context"
	"errors"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	schema "github.com/mutablelogic/go-filer/store/sqlite/schema"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	sqlite "github.com/mutablelogic/go-sqlite"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

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
