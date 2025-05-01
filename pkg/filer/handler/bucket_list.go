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

func bucketList(w http.ResponseWriter, r *http.Request, filer filer.Filer) error {
	// Request
	var req schema.BucketListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List buckets
	buckets, err := filer.ListBuckets(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), buckets)
}
