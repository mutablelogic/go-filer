package httphandler

import (
	"errors"
	"net/http"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	manager "github.com/mutablelogic/go-filer/filer/manager"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// RegisterSearchHandlers registers the search handlers
func RegisterSearchHandlers(manager *manager.Filer, router *httprouter.Router) error {
	router.Spec().AddTag("Search", documentation.Section(2, "Search Operations").Body)
	return errors.Join(
		router.Register("search", nil, func(path httprequest.PathItem) {
			path.Tag("Search")

			// GET
			path.Get(func(w http.ResponseWriter, r *http.Request) {
				var req schema.SearchListRequest
				if err := httprequest.Query(r.URL.Query(), &req); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
				} else if resp, err := manager.Search(r.Context(), req); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Search")
				op.Description(documentation.Section(3, "GET /search").Body)
				op.Query(jsonschema.MustFor[schema.SearchListRequest]())
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.SearchList](), "List of search results")
				op.ErrorResponse(http.StatusBadRequest, "Invalid request, or missing search query")
			})
		}),
	)
}
