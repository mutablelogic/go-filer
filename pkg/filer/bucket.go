package filer

import (
	"context"
	"net/http"

	// Packages
	"github.com/mutablelogic/go-filer/pkg/filer/handler"
	"github.com/mutablelogic/go-server"
	"github.com/mutablelogic/go-server/pkg/httpresponse"
	"github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (filer *filer) RegisterBucketHandlers(ctx context.Context, prefix string, router server.HTTPRouter) {
	// Create or List buckets
	router.HandleFunc(ctx, types.JoinPath(prefix, "bucket"), func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		httpresponse.Cors(w, r, router.Origin(), http.MethodGet, http.MethodPost)

		switch r.Method {
		case http.MethodPost:
			_ = handler.BucketCreate(w, r, filer.aws)
		case http.MethodGet:
			_ = handler.BucketList(w, r, filer.aws)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// Get or delete bucket
	router.HandleFunc(ctx, types.JoinPath(prefix, "bucket/{bucket}"), func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		httpresponse.Cors(w, r, router.Origin(), http.MethodGet, http.MethodDelete)

		switch r.Method {
		case http.MethodGet:
			_ = handler.BucketGet(w, r, filer.aws, r.PathValue("bucket"))
		case http.MethodDelete:
			_ = handler.BucketDelete(w, r, filer.aws, r.PathValue("bucket"))
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}

	})
}
