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

// VolumePut is a volume which is put: created, or its URL, and whether
// it's enabled, replaced
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

// Insert binds a volume, and returns the query which puts it, which only
// modifies it when it changes
func (put VolumePut) Insert(bind *sqlite.Bind) (string, error) {
	bind.Set("name", put.Name)
	bind.Set("url", put.URL)
	bind.Set("enabled", put.Enabled)
	bind.Set("now", time.Now().UTC())
	return bind.Query("volume.put"), nil
}

func (put VolumePut) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("a volume is put")
}

func (state VolumeState) Insert(*sqlite.Bind) (string, error) {
	return "", sqlite.ErrNotImplemented.With("a volume's state is updated")
}

// Update binds the state, whose zero values are NULL
func (state VolumeState) Update(bind *sqlite.Bind) error {
	bind.Set("status", string(state.Status))
	bind.Set("folders", state.Folders)
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
	return row.Scan(&state.Status, &state.Folders, &state.Started, &state.Scanned, &state.Error)
}
