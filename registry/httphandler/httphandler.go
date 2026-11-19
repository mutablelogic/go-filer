package httphandler

import (
	_ "embed"
	"errors"

	// Packages
	registry "github.com/mutablelogic/go-filer/registry"
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
// with the router. The changes to their objects are streamed by the
// broadcaster's owner, as go-server's /changes.
func RegisterHandlers(reg *registry.Registry, router *httprouter.Router) error {
	var documentation = openapi.ParseMarkdown(readme)
	router.Spec().AddTagGroup("Filer", "Volumes", "Objects")
	return errors.Join(
		volumeHandlers(reg, router, documentation),
		objectHandlers(reg, router, documentation),
	)
}
