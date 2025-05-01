package handler

import (
	"net/http"
	"net/url"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	aws "github.com/mutablelogic/go-filer/pkg/aws"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func objectList(w http.ResponseWriter, r *http.Request, filer filer.AWS, bucket string) error {
	// Request options
	var req req
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List objects
	objects, err := filer.ListObjects(r.Context(), bucket, req.Opts()...)
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

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

type req struct {
	Prefix *string `json:"prefix,omitempty"`
}

func (r *req) Opts() []aws.Opt {
	result := []aws.Opt{}
	if r.Prefix != nil {
		result = append(result, aws.WithPrefix(*r.Prefix))
	}
	return result
}
