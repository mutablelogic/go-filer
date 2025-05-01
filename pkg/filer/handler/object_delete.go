package handler

import (
	"net/http"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func objectDelete(w http.ResponseWriter, r *http.Request, filer filer.AWS, bucket, key string) error {
	err := filer.DeleteObject(r.Context(), bucket, key)
	if err != nil {
		return httpresponse.Error(w, err, types.JoinPath(bucket, key))
	}
	return httpresponse.Empty(w, http.StatusOK)
}
