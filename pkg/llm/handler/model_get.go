package handler

import (
	"net/http"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/llm/schema"
	llm "github.com/mutablelogic/go-llm"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func modelGet(w http.ResponseWriter, r *http.Request, llm llm.Agent, name string) error {
	// Get model
	model := llm.Model(r.Context(), name)
	if model == nil {
		return httpresponse.Error(w, httpresponse.ErrNotFound, name)
	}

	// Return response
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema.NewModel(model))
}
