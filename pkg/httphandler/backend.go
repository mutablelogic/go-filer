package httphandler

import (
	"net/http"

	// Packages
	manager "github.com/mutablelogic/go-filer/pkg/manager"
	schema "github.com/mutablelogic/go-filer/pkg/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	openapi "github.com/mutablelogic/go-server/pkg/openapi/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// HANDLER FUNCTIONS

// Path: /{$}
// GET returns the list of registered backends.
func BackendListHandler(mgr *manager.Manager) (string, http.HandlerFunc, *openapi.PathItem) {
	return "/{$}", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				_ = backendList(w, r, mgr)
			default:
				_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
			}
		}, types.Ptr(openapi.PathItem{
			Get: &openapi.Operation{
				Description: "List all registered filer backends",
			},
		})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func backendList(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	backends := mgr.Backends()
	body := make(map[string]string, len(backends))
	for _, name := range backends {
		if b := mgr.Backend(name); b != nil {
			body[name] = b.URL().String()
		}
	}
	response := schema.BackendListResponse{Body: body}
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}
