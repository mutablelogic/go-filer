package httphandler

import (
	"errors"
	"net/http"
	"path"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// objectName is the path of the objects in a volume
type objectName struct {
	Name string `json:"name" help:"Volume name"`
}

// objectListQuery is the query for listing objects
type objectListQuery struct {
	Path       string  `json:"path,omitempty" help:"Directory to list, which defaults to the root of the volume"`
	Recursive  bool    `json:"recursive,omitempty" help:"List every object beneath the directory"`
	StartAfter string  `json:"start_after,omitempty" help:"Return objects whose keys sort after this key"`
	Limit      *uint64 `json:"limit,omitempty" help:"Maximum number of objects to return, which defaults to and is capped at 100"`
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func objectHandlers(registry *filer.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	router.Spec().AddTag("Objects", documentation.Section(2, "Object Operations").Body)
	return errors.Join(
		objectListHandlers(registry, router, documentation),
	)
}

func objectListHandlers(registry *filer.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("object/{name}", jsonschema.MustFor[objectName](), func(path httprequest.PathItem) {
		path.Tag("Objects")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			var query objectListQuery
			if err := httprequest.Query(r.URL.Query(), &query); err != nil {
				httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("%w", err))
			} else if key, err := cleanPath(query.Path); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("name")+"/"+query.Path)
			} else {
				req := schema.ObjectListRequest{Limit: query.Limit}
				req.Name, req.Path = r.PathValue("name"), key
				req.Recursive, req.StartAfter = query.Recursive, query.StartAfter
				if list, err := registry.List(r.Context(), req); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), req.Name+req.Path)
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), list)
				}
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("List Objects")
			op.Description(documentation.Section(3, "GET /object/{name}").Body)
			op.Query(jsonschema.MustFor[objectListQuery]())
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.ObjectList](), "Objects")
			op.ErrorResponse(http.StatusBadRequest, "Invalid path or query")
			op.ErrorResponse(http.StatusNotFound, "Volume isn't mounted, or path not found")
		})
	})
}

// cleanPath returns the path of an object, which starts with "/", resolving
// "." and ".." elements, or ErrBadParameter when it's outside the volume
func cleanPath(key string) (string, error) {
	clean := path.Clean(strings.TrimPrefix(key, "/"))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", gofiler.ErrBadParameter.Withf("path is outside the volume: %q", key)
	} else if clean == "." {
		return "/", nil
	}
	return "/" + clean, nil
}
