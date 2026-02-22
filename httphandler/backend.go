package httphandler

import (
	"net/http"

	// Packages
	manager "github.com/djthorpe/go-filer/manager"
	schema "github.com/mutablelogic/go-filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func backendList(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	response := schema.BackendListResponse{
		Body: manager.Backends(),
	}
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}
