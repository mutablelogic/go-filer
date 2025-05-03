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

func urlGet(w http.ResponseWriter, r *http.Request, feed filer.Feed, id uint64) error {
	url, err := feed.GetUrl(r.Context(), id)
	if err != nil {
		return httpresponse.Error(w, err)
	}
	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), url)
}
