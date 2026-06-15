package manager

import (
	"context"
	"net/url"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/extractor/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

type volumeCreate struct {
	URL string `json:"url,omitempty"`
	schema.VolumeMeta
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) CreateVolume(ctx context.Context, url *url.URL, meta schema.VolumeMeta) (_ *schema.Volume, err error) {
	// Check parameters
	if url == nil {
		return nil, gofiler.ErrBadParameter.With("url is required")
	}
	return nil, gofiler.ErrNotImplemented
}
