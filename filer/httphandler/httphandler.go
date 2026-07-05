package httphandler

import (
	_ "embed"
	"errors"

	// Packages
	manager "github.com/mutablelogic/go-filer/filer/manager"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed README.md
var readme []byte
var documentation = openapi.ParseMarkdown(readme)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterHandlers(manager *manager.Filer, router *httprouter.Router) error {
	return errors.Join(
		RegisterVolumeHandlers(manager, router),
		RegisterObjectHandlers(manager, router),
		RegisterSearchHandlers(manager, router),
		RegisterMetadataHandlers(manager, router),
		RegisterArtworkHandlers(manager, router),
		RegisterLLMProviderHandlers(manager, router),
	)
}
