package httphandler

import (
	_ "embed"
	"errors"

	// Packages
	filer "github.com/mutablelogic/go-filer/filer"
	changes "github.com/mutablelogic/go-server/broadcaster/httphandler"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed README.md
var readme []byte

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// RegisterHandlers registers the filer API, for the volumes of the registry,
// with the router, and go-server's /changes, which streams the changes to
// their objects, when the registry's broadcaster is its own, as one which is
// shared is served by its owner
func RegisterHandlers(registry *filer.Registry, router *httprouter.Router) error {
	var documentation = openapi.ParseMarkdown(readme)
	if registry.Shared() {
		router.Spec().AddTagGroup("Filer", "Volumes", "Objects")
		return errors.Join(
			volumeHandlers(registry, router, documentation),
			objectHandlers(registry, router, documentation),
		)
	}
	router.Spec().AddTagGroup("Filer", "Volumes", "Objects", changes.Tag)
	return errors.Join(
		volumeHandlers(registry, router, documentation),
		objectHandlers(registry, router, documentation),
		changes.RegisterHandlers(registry.Broadcaster(), router),
	)
}
