package handler

import (
	"net/http"

	// Packages
	plugin "github.com/mutablelogic/go-filer/plugin"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func BucketDelete(w http.ResponseWriter, r *http.Request, client plugin.AWS, name string) error {
	err := client.DeleteBucket(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.Empty(w, http.StatusOK)
}
