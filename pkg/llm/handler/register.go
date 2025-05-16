package handler

import (
	"context"
	"net/http"

	// Packages

	llm "github.com/mutablelogic/go-llm"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func RegisterHandlers(ctx context.Context, prefix string, router server.HTTPRouter, llm llm.Agent) {
	registerModelHandlers(ctx, prefix, router, llm)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS - MODELS

func registerModelHandlers(ctx context.Context, prefix string, router server.HTTPRouter, llm llm.Agent) {
	// List models
	router.HandleFunc(ctx, types.JoinPath(prefix, "model"), func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		httpresponse.Cors(w, r, router.Origin(), http.MethodGet)

		switch r.Method {
		case http.MethodOptions:
			_ = httpresponse.Empty(w, http.StatusOK)
		case http.MethodGet:
			_ = modelList(w, r, llm)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// Get model
	router.HandleFunc(ctx, types.JoinPath(prefix, "model/{name...}"), func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		httpresponse.Cors(w, r, router.Origin(), http.MethodGet)

		switch r.Method {
		case http.MethodOptions:
			_ = httpresponse.Empty(w, http.StatusOK)
		case http.MethodGet:
			_ = modelGet(w, r, llm, r.PathValue("name"))
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}
