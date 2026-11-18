package httphandler

import (
	_ "embed"
	"errors"

	// Packages
	filer "github.com/mutablelogic/go-filer/filer"
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
func RegisterHandlers(registry *filer.Registry, router *httprouter.Router) error {
	var documentation = openapi.ParseMarkdown(readme)
	router.Spec().AddTagGroup("Filer", "Volumes", "Objects", "Sign In")
	return errors.Join(
		volumeHandlers(registry, router, documentation),
		objectHandlers(registry, router, documentation),
		signInHandlers(registry, router, documentation),
	)
}
