package handler

import (
	"net/http"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/llm/schema"
	llm "github.com/mutablelogic/go-llm"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func modelList(w http.ResponseWriter, r *http.Request, llm llm.Agent) error {
	// Request
	var req schema.ModelListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Get models
	models, err := llm.Models(r.Context())
	if err != nil {
		return httpresponse.Error(w, err)
	}

	// Build response
	var resp schema.ModelList
	resp.Count = uint64(len(models))
	resp.Body = make([]*schema.Model, 0, len(models))
	for i := uint64(0); i < resp.Count; i++ {
		if i < req.Offset {
			continue
		}
		if req.Limit != nil && i >= req.Offset+types.PtrUint64(req.Limit) {
			break
		}
		resp.Body = append(resp.Body, schema.NewModel(models[i]))
	}

	// Return models
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
}
