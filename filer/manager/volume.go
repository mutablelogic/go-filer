package manager

import (
	"context"
	"net/url"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) CreateVolume(ctx context.Context, url *url.URL, meta schema.VolumeMeta) (_ *schema.Volume, err error) {
	// Check that the backend can be handled, and return the name
	name, err := manager.Registry.Validate(url)
	if err != nil {
		return nil, err
	}

	// Insert the volume record in the database - which in tern syncs the volume with the registry
	var result schema.Volume
	if err := manager.With("name", name).Insert(ctx, &result, schema.VolumeCreate{
		URL:        url.String(),
		VolumeMeta: meta,
	}); err != nil {
		return nil, err
	}

	// Return the created volume record
	return &result, nil
}
