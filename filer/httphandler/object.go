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

func RegisterObjectHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Objects", "Object Operations")

	return errors.Join(
		router.RegisterPath("object", nil, httprequest.NewPathItem("Objects", "Manage objects").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListObjects(w, r, manager)
				},
				"List objects",
				openapi.WithTags("Objects"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.ObjectListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.ObjectList]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListObjects(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.ObjectListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if resp, err := manager.ListObjects(r.Context(), req); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}
}
