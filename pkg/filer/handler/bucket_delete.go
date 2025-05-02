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

func bucketDelete(w http.ResponseWriter, r *http.Request, client filer.Filer, bucket string) error {
	// Request parameters
	var req struct {
		Force bool `json:"force,omitempty" help:"Force delete the bucket and all objects in it"`
	}
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err, bucket)
	}

	// Delete the bucket
	_, err := client.DeleteBucket(r.Context(), bucket, filer.WithForce(req.Force))
	if err != nil {
		return httpresponse.Error(w, err)
	}

	// Return success
	return httpresponse.Empty(w, http.StatusOK)
}
