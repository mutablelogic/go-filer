package httphandler

import (
	"errors"
	"net/http"

	// Packages
	manager "github.com/mutablelogic/go-filer/pkg/manager"
	openapi "github.com/mutablelogic/go-server/pkg/openapi/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Router is the interface required to register HTTP handlers.
type Router interface {
	RegisterFunc(path string, handler http.HandlerFunc, middleware bool, spec *openapi.PathItem) error
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterHandlers registers all filer HTTP handlers on the provided router.
func RegisterHandlers(mgr *manager.Manager, router Router) error {
	var result error
	register := func(path string, handler http.HandlerFunc, spec *openapi.PathItem) {
		result = errors.Join(result, router.RegisterFunc(path, handler, true, spec))
	}
	// Register backend list at both /{$} (matches /api/filer/) and "" (matches
	// /api/filer exactly), so http.ServeMux doesn't redirect the bare prefix.
	path, handler, spec := BackendListHandler(mgr)
	register(path, handler, spec)
	register("", handler, nil)
	register(ObjectListHandler(mgr))
	register(ObjectHandler(mgr))
	return result
}
