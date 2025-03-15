package handler

import (
	"net/http"

	// Packages
	"github.com/mutablelogic/go-filer/pkg/aws"
	"github.com/mutablelogic/go-filer/pkg/filer/schema"
	"github.com/mutablelogic/go-filer/plugin"
	"github.com/mutablelogic/go-server/pkg/httprequest"
	"github.com/mutablelogic/go-server/pkg/httpresponse"
	"github.com/mutablelogic/go-server/pkg/types"
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
	bucket, err := aws.CreateBucket(r.Context(), client.S3(), req.Name, opts...)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), schema.BucketFromAWS(bucket))
}
