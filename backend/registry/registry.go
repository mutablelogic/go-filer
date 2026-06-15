package registry

import (
	"net/url"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	backend "github.com/mutablelogic/go-filer/backend"
	file "github.com/mutablelogic/go-filer/backend/file"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Registry struct {
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func (Registry) NewBackend(url *url.URL) (backend.Backend, error) {
	if url == nil || url.Scheme == "" {
		return nil, gofiler.ErrBadParameter.With("url with scheme is required")
	}
	// Add cases for different backend types here
	switch url.Scheme {
	case "file":
		if backend, err := file.New(url); err != nil {
			return nil, err
		} else {
			return backend, nil
		}
	default:
		return nil, gofiler.ErrBadParameter.Withf("unsupported backend scheme: %s", url.Scheme)
	}
}
