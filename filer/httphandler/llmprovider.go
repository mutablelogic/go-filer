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
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// RegisterLLMProviderHandlers registers the LLM provider handlers
func RegisterLLMProviderHandlers(manager *manager.Filer, router *httprouter.Router) error {
	router.Spec().AddTag("LLM Providers", documentation.Section(2, "Large Language Model (LLM) Operations").Body)
	return errors.Join(
		router.Register("llmprovider", nil, func(path httprequest.PathItem) {
			path.Tag("LLM Providers")

			// POST
			path.Post(func(w http.ResponseWriter, r *http.Request) {
				var req schema.LLMProviderCreate
				if err := httprequest.Read(r, &req); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
				} else if resp, err := manager.CreateLLMProvider(r.Context(), req); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), req.String())
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Create LLM Provider")
				op.Description(documentation.Section(3, "POST /llmprovider").Body)
				op.RequestBody(jsonschema.MustFor[schema.LLMProviderCreate]())
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.LLMProvider](), "Created LLM provider")
				op.ErrorResponse(http.StatusBadRequest, "Invalid request, or unsupported or unreachable provider")
				op.ErrorResponse(http.StatusConflict, "A provider with that name already exists")
			})
		}),
	)
}
