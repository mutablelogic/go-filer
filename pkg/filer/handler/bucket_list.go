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

func BucketList(w http.ResponseWriter, r *http.Request, client plugin.AWS) error {
	buckets, err := client.ListBuckets(r.Context())
	if err != nil {
		return httpresponse.Error(w, err)
	}

	// Return the list of buckets
	result := make([]*schema.Bucket, len(buckets))
	for i, bucket := range buckets {
		result[i] = schema.BucketFromAWS(&bucket)
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), result)
}
