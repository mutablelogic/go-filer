package handler

import (
	"context"
	"net/http"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func RegisterHandlers(ctx context.Context, prefix string, router server.HTTPRouter, filer filer.Filer) {
	registerBucketHandlers(ctx, prefix, router, filer)
	// registerObjectHandlers(ctx, prefix, router, filer)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS - BUCKETS

func registerBucketHandlers(ctx context.Context, prefix string, router server.HTTPRouter, filer filer.Filer) {
	// Create or List buckets
	router.HandleFunc(ctx, types.JoinPath(prefix, "bucket"), func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		httpresponse.Cors(w, r, router.Origin(), http.MethodGet, http.MethodPost)

		switch r.Method {
		case http.MethodPost:
			_ = bucketCreate(w, r, filer)
		case http.MethodGet:
			_ = bucketList(w, r, filer)
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
			_ = bucketGet(w, r, filer, r.PathValue("bucket"))
		case http.MethodDelete:
			_ = bucketDelete(w, r, filer, r.PathValue("bucket"))
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}

	})
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS - OBJECTS

func registerObjectHandlers(ctx context.Context, prefix string, router server.HTTPRouter, filer filer.AWS) {
	// List objects in a bucket
	// Create or update objects in a bucket
	router.HandleFunc(ctx, types.JoinPath(prefix, "object/{bucket}"), func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		httpresponse.Cors(w, r, router.Origin(), http.MethodGet, http.MethodPost)

		switch r.Method {
		case http.MethodGet:
			_ = objectList(w, r, filer, r.PathValue("bucket"))
		case http.MethodPost:
			_ = objectCreate(w, r, filer, r.PathValue("bucket"))
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// Get or delete object
	router.HandleFunc(ctx, types.JoinPath(prefix, "object/{bucket}/{key...}"), func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		httpresponse.Cors(w, r, router.Origin(), http.MethodGet, http.MethodDelete)

		switch r.Method {
		case http.MethodGet:
			_ = objectHead(w, r, filer, r.PathValue("bucket"), r.PathValue("key"))
		case http.MethodDelete:
			_ = objectDelete(w, r, filer, r.PathValue("bucket"), r.PathValue("key"))
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}
