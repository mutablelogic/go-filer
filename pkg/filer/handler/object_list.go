package handler

import (
	"net/http"
	"net/url"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	plugin "github.com/mutablelogic/go-filer/plugin"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ObjectList(w http.ResponseWriter, r *http.Request, client plugin.AWS, bucket string) error {
	objects, err := client.ListObjects(r.Context(), bucket)
	if err != nil {
		return httpresponse.Error(w, err, bucket)
	}

	// Create response
	result := make([]*schema.Object, len(objects))
	for i, object := range objects {
		result[i] = schema.ObjectFromAWS(&object, bucket, url.Values{})
	}

	// Return response
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), result)
}
