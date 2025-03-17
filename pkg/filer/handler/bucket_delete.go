package handler

import (
	"net/http"

	// Packages
	plugin "github.com/mutablelogic/go-filer"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func BucketDelete(w http.ResponseWriter, r *http.Request, client plugin.AWS, bucket string) error {
	err := client.DeleteBucket(r.Context(), bucket)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.Empty(w, http.StatusOK)
}
