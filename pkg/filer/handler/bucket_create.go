package handler

import (
	"net/http"

	// Packages
	plugin "github.com/mutablelogic/go-filer"
	aws "github.com/mutablelogic/go-filer/pkg/aws"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func BucketCreate(w http.ResponseWriter, r *http.Request, client plugin.AWS) error {
	// Read request
	var req schema.BucketMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Set region
	opts := []aws.Opt{}
	if region := types.PtrString(req.Region); region == "" {
		opts = append(opts, aws.WithRegion(client.Region()))
	} else {
		opts = append(opts, aws.WithRegion(region))
	}

	// Create bucket
	bucket, err := client.CreateBucket(r.Context(), req.Name, opts...)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), schema.BucketFromAWS(bucket))
}
