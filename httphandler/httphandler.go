package httphandler

import (
	"net/http"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type HTTPMiddlewareFuncs []func(http.HandlerFunc) http.HandlerFunc

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterHandlers registers all filer HTTP handlers on the provided
// router with the given path prefix. The manager must be non-nil.
func RegisterHandlers(router *http.ServeMux, prefix string, manager *filer.Manager, middleware HTTPMiddlewareFuncs) {
	// GET /api/filer - list backends
	router.HandleFunc(joinPath(prefix, ""), middleware.Wrap(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = backendList(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	}))

	// GET /api/filer/{scheme}/{host} - list objects at backend root
	router.HandleFunc(joinPath(prefix, "/{scheme}/{host}"), middleware.Wrap(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = objectList(w, r, manager, prefix)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	}))

	// GET /api/filer/{scheme}/{host}/{path...} - get a single file, with metadata in X-Object-Meta encoded as JSON
	// HEAD /api/filer/{scheme}/{host}/{path...} - get file metadata without body
	// PUT /api/filer/{scheme}/{host}/{path...} - create an object at path (TODO: do multipart upload of multiple files when Content-Type is multipart/form-data)
	// DELETE /api/filer/{scheme}/{host}/{path...} - delete an object at path and return the deleted file metadata (TODO: recursive when path is a "directory")
	router.HandleFunc(joinPath(prefix, "/{scheme}/{host}/{path...}"), middleware.Wrap(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = objectGet(w, r, manager, prefix)
		case http.MethodHead:
			_ = objectHead(w, r, manager, prefix)
		case http.MethodPut:
			_ = objectPut(w, r, manager, prefix)
		case http.MethodDelete:
			_ = objectDelete(w, r, manager, prefix)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	}))
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (w HTTPMiddlewareFuncs) Wrap(handler http.HandlerFunc) http.HandlerFunc {
	if len(w) == 0 {
		return handler
	}
	for i := len(w) - 1; i >= 0; i-- {
		handler = w[i](handler)
	}
	return handler
}

func joinPath(prefix, path string) string {
	return types.JoinPath(prefix, path)
}
