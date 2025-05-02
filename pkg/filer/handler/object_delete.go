package handler

import (
	"net/http"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func objectDelete(w http.ResponseWriter, r *http.Request, filer filer.Filer, bucket, key string) error {
	_, err := filer.DeleteObject(r.Context(), bucket, key)
	if err != nil {
		return httpresponse.Error(w, err, bucket, key)
	}
	return httpresponse.Empty(w, http.StatusOK)
}
