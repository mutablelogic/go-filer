package handler

import (
	"net/http"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func urlUpdate(w http.ResponseWriter, r *http.Request, feed filer.Feed, id uint64) error {
	// Read request
	var req schema.UrlMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Update url
	url, err := feed.UpdateUrl(r.Context(), id, req)
	if err != nil {
		return httpresponse.Error(w, err, req)
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), url)
}
