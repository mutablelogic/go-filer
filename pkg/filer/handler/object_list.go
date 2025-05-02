package handler

import (
	"net/http"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func objectList(w http.ResponseWriter, r *http.Request, filer filer.Filer, bucket string) error {
	// Request options
	var req schema.ObjectListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List objects
	resp, err := filer.ListObjects(r.Context(), bucket, req)
	if err != nil {
		return httpresponse.Error(w, err, bucket)
	}

	// Return response
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
}
