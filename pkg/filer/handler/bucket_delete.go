package handler

import (
	"net/http"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func bucketDelete(w http.ResponseWriter, r *http.Request, filer filer.Filer, bucket string) error {
	_, err := filer.DeleteBucket(r.Context(), bucket)
	if err != nil {
		return httpresponse.Error(w, err)
	}
	return httpresponse.Empty(w, http.StatusOK)
}
