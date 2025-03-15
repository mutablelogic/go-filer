package handler

import (
	"net/http"

	// Packages
	aws "github.com/mutablelogic/go-filer/pkg/aws"
	plugin "github.com/mutablelogic/go-filer/plugin"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func BucketDelete(w http.ResponseWriter, r *http.Request, client plugin.AWS, name string) error {
	err := aws.DeleteBucket(r.Context(), client.S3(), name)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.Empty(w, http.StatusOK)
}
