package store

import (
	"context"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// VolumeStore is where the volumes are kept, so they outlast a restart,
// which mounts those which are enabled. It's safe to use from more than one
// goroutine.
type VolumeStore interface {
	// CreateVolume stores a new volume, and returns it, with when it was
	// created, or returns ErrConflict when a volume of its name is stored
	CreateVolume(ctx context.Context, volume schema.VolumeMeta) (*schema.Volume, error)

	// UpdateVolume replaces a stored volume's URL, and whether it's enabled,
	// keeping its index, and returns it, or returns ErrNotFound when it
	// isn't stored. It's only modified when they change.
	UpdateVolume(ctx context.Context, volume schema.VolumeMeta) (*schema.Volume, error)

	// GetVolume returns a volume, or ErrNotFound
	GetVolume(ctx context.Context, name string) (*schema.Volume, error)

	// ListVolumes returns the volumes, in the order of their names
	ListVolumes(ctx context.Context) ([]*schema.Volume, error)

	// DeleteVolume removes a volume, with its index, or returns ErrNotFound
	DeleteVolume(ctx context.Context, name string) error
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// ValidateVolume returns ErrBadParameter when a volume's name isn't an
// identifier, or it has no URL
func ValidateVolume(volume schema.VolumeMeta) error {
	switch {
	case !types.IsIdentifier(volume.Name):
		return gofiler.ErrBadParameter.Withf("invalid volume name: %q", volume.Name)
	case strings.TrimSpace(volume.URL) == "":
		return gofiler.ErrBadParameter.Withf("volume %q has no url", volume.Name)
	}
	return nil
}
