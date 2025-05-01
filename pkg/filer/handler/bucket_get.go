package handler

import (
	"net/http"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func bucketGet(w http.ResponseWriter, r *http.Request, filer filer.Filer, name string) error {
	bucket, err := filer.GetBucket(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), bucket)
}
