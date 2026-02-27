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

// Path: /{name}
// GET lists objects at the backend root. POST uploads via multipart/form-data
// (field name: "file", repeatable for multiple files) to the backend root.
func ObjectListHandler(mgr *manager.Manager) (string, http.HandlerFunc, *openapi.PathItem) {
	return "/{name}", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				_ = objectList(w, r, mgr)
			case http.MethodPost:
				_ = objectUpload(w, r, mgr)
			default:
				_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
			}
		}, types.Ptr(openapi.PathItem{
			Get: &openapi.Operation{
				Description: "List objects at the backend root",
			},
			Post: &openapi.Operation{
				Description: "Upload one or more files using multipart/form-data (field name: \"file\", repeatable)",
			},
		})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func objectList(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	var request schema.ListObjectsRequest

	// Read query parameters into request struct
	if err := httprequest.Query(r.URL.Query(), &request); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}

	// Normalise path for consistency with object handlers
	request.Path = types.NormalisePath(request.Path)

	// Get the list of objects from the manager
	response, err := mgr.ListObjects(r.Context(), r.PathValue("name"), request)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	// Return the response as JSON
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}
