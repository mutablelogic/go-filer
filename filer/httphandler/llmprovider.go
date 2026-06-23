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
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterLLMProviderHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("LLM Providers", "LLM Provider Operations")

	return errors.Join(
		router.RegisterPath("llmprovider", nil, httprequest.NewPathItem("LLM Providers", "Manage LLM providers").
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateLLMProvider(w, r, manager)
				},
				"Create a LLM provider",
				openapi.WithTags("LLM Providers"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.LLMProviderCreate]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.LLMProvider]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func CreateLLMProvider(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.LLMProviderCreate
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if resp, err := manager.CreateLLMProvider(r.Context(), req); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), req.String())
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}
}
