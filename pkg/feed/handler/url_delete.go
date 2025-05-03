package handler

import (
	"net/http"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func urlDelete(w http.ResponseWriter, r *http.Request, feed filer.Feed, id uint64) error {
	_, err := feed.DeleteUrl(r.Context(), id)
	if err != nil {
		return httpresponse.Error(w, err)
	}
	// Return success
	return httpresponse.Empty(w, http.StatusOK)
}
