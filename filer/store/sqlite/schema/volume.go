package schema

import (
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer/filer/schema"
	sqlite "github.com/mutablelogic/go-sqlite"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// VolumeName selects a volume, of its name, which is got, or deleted
type VolumeName string

// VolumeListRequest selects every volume, in the order of their names
type VolumeListRequest struct{}

// VolumeList is the volumes which are read, of a list, or a get, which reads
// one
type VolumeList []*filer.Volume

// VolumePut is a volume which is created, or whose URL, and whether it's
// enabled, are updated
type VolumePut filer.VolumeMeta

// VolumeState is the state of a volume's index, of its name, which is got,
// and updated
type VolumeState struct {
	Name string
	filer.IndexState
}

////////////////////////////////////////////////////////////////////////////////
// SELECTORS

func (name VolumeName) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	bind.Set("name", string(name))
	switch op {
	case sqlite.Get:
		return bind.Query("volume.get"), nil
	case sqlite.Update:
		return bind.Query("volume.update"), nil
	case sqlite.Delete:
		return bind.Query("volume.delete"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("volume: %v", op)
}

func (VolumeListRequest) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	switch op {
	case sqlite.List:
		return bind.Query("volume.list"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("volume: %v", op)
}

func (state VolumeState) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	bind.Set("name", state.Name)
	switch op {
	case sqlite.Get:
		return bind.Query("volume.state"), nil
	case sqlite.Update:
		return bind.Query("volume.set_state"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("volume state: %v", op)
}

////////////////////////////////////////////////////////////////////////////////
// WRITERS

// Insert binds a volume, and returns the query which creates it
func (put VolumePut) Insert(bind *sqlite.Bind) (string, error) {
	put.Update(bind)
	bind.Set("name", put.Name)
	return bind.Query("volume.create"), nil
}

// Update binds a volume's URL, and whether it's enabled
func (put VolumePut) Update(bind *sqlite.Bind) error {
	bind.Set("url", put.URL)
	bind.Set("enabled", put.Enabled)
	bind.Set("now", time.Now().UTC())
	return nil
}

func (state VolumeState) Insert(*sqlite.Bind) (string, error) {
	return "", sqlite.ErrNotImplemented.With("a volume's state is updated")
}

// Update binds the state, whose zero values are NULL
func (state VolumeState) Update(bind *sqlite.Bind) error {
	bind.Set("status", string(state.Status))
	bind.Set("folders", state.Folders)
	bind.Set("files", state.Files)
	bind.Set("started", state.Started)
	bind.Set("scanned", state.Scanned)
	bind.Set("error", state.Error)
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// READERS

func (list *VolumeList) Scan(row sqlite.Row) error {
	var volume filer.Volume
	if err := row.Scan(&volume.Name, &volume.URL, &volume.Enabled, &volume.Created, &volume.Modified); err != nil {
		return err
	}
	*list = append(*list, &volume)
	return nil
}

func (state *VolumeState) Scan(row sqlite.Row) error {
	return row.Scan(&state.Status, &state.Folders, &state.Files, &state.Started, &state.Scanned, &state.Error)
}
