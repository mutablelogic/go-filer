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

func (filer *filer) RegisterObjectHandlers(ctx context.Context, prefix string, router server.HTTPRouter) {
	// List objects in a bucket
	router.HandleFunc(ctx, types.JoinPath(prefix, "object/{bucket...}"), func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		httpresponse.Cors(w, r, router.Origin(), http.MethodGet)

		switch r.Method {
		case http.MethodGet:
			_ = handler.ObjectList(w, r, filer.aws, r.PathValue("bucket"))
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}
