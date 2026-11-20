package httphandler

import (
	"net/http"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	registry "github.com/mutablelogic/go-filer/registry"
	schema "github.com/mutablelogic/go-filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// searchQuery is the query of a search, of its text, and its page
type searchQuery struct {
	Query  string  `json:"query" help:"What's searched for: words, any of which match, quoted phrases, the starts of words, ending with *, and words which don't match, starting with -, such as arvo \"spiegel im spiegel\" beethov* -symphony"`
	Offset uint64  `json:"offset,omitempty" help:"How many of the objects which match are skipped, before those which are returned"`
	Limit  *uint64 `json:"limit,omitempty" help:"Maximum number of objects to return, which defaults to and is capped at 100"`
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func searchHandlers(reg *registry.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("search", nil, func(path httprequest.PathItem) {
		path.Tag("Objects")
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			var query searchQuery
			if err := httprequest.Query(r.URL.Query(), &query); err != nil {
				httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("%w", err))
			} else if result, err := reg.Search(r.Context(), query.Query, schema.OffsetLimit{Offset: query.Offset, Limit: query.Limit}); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err))
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), result)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Search Objects")
			op.Description(documentation.Section(3, "GET /search").Body)
			op.Query(jsonschema.MustFor[searchQuery]())
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.SearchResponse](), "Objects which match, best first")
			op.ErrorResponse(http.StatusBadRequest, "Invalid query, such as a quote which doesn't end")
		})
	})
}
