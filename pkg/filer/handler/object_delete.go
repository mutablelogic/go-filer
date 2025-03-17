package handler

import (
	"net/http"

	// Packages
	plugin "github.com/mutablelogic/go-filer"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ObjectDelete(w http.ResponseWriter, r *http.Request, client plugin.AWS, bucket, key string) error {
	err := client.DeleteObject(r.Context(), bucket, key)
	if err != nil {
		return httpresponse.Error(w, err, types.JoinPath(bucket, key))
	}
	return httpresponse.Empty(w, http.StatusOK)
}
