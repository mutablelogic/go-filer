package handler

import (
	"context"
	"net/http"
	"strconv"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func RegisterHandlers(ctx context.Context, router server.HTTPRouter, feed filer.Feed) {
	registerUrlHandlers(ctx, router, schema.APIPrefix, feed)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS - URL

func registerUrlHandlers(ctx context.Context, router server.HTTPRouter, prefix string, feed filer.Feed) {
	// Create or List urls
	router.HandleFunc(ctx, types.JoinPath(prefix, "url"), func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		httpresponse.Cors(w, r, router.Origin(), http.MethodGet, http.MethodPost)

		switch r.Method {
		case http.MethodOptions:
			_ = httpresponse.Empty(w, http.StatusOK)
		case http.MethodPost:
			_ = urlCreate(w, r, feed)
		case http.MethodGet:
			_ = urlList(w, r, feed)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// Get or delete url
	router.HandleFunc(ctx, types.JoinPath(prefix, "url/{id}"), func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		httpresponse.Cors(w, r, router.Origin(), http.MethodGet, http.MethodDelete)

		id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
		if err != nil {
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusBadRequest), err, r.PathValue("id"))
			return
		}

		switch r.Method {
		case http.MethodOptions:
			_ = httpresponse.Empty(w, http.StatusOK)
		case http.MethodGet:
			_ = urlGet(w, r, feed, id)
		case http.MethodDelete:
			_ = urlDelete(w, r, feed, id)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}
