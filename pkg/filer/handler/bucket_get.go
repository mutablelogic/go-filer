package handler

import (
	"net/http"

	// Packages
	aws "github.com/mutablelogic/go-filer/pkg/aws"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	plugin "github.com/mutablelogic/go-filer/plugin"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func BucketGet(w http.ResponseWriter, r *http.Request, client plugin.AWS, name string) error {
	bucket, err := aws.GetBucket(r.Context(), client.S3(), name)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema.BucketFromAWS(bucket))
}
