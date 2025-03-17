package handler

import (
	"net/http"

	// Packages
	plugin "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func BucketGet(w http.ResponseWriter, r *http.Request, client plugin.AWS, name string) error {
	bucket, err := client.GetBucket(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema.BucketFromAWS(bucket))
}
