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
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterSearchHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Search", "Search Operations")

	return errors.Join(
		router.RegisterPath("search", nil, httprequest.NewPathItem("Search", "List search results").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListSearchResults(w, r, manager)
				},
				"List search results",
				openapi.WithTags("Search"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.SearchListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.SearchList]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListSearchResults(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.SearchListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if resp, err := manager.Search(r.Context(), req); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}
}
