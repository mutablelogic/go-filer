package httphandler

import (
	"errors"
	"net/http"

	// Packages
	manager "github.com/mutablelogic/go-filer/filer/manager"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func RegisterHandlers(router *httprouter.Router, mgr *manager.Manager) error {
	return errors.Join(
		RegisterConfigHandlers(router, mgr),
		RegisterListHandlers(router, mgr),
		RegisterResourceHandlers(router, mgr),
	)
}

func RegisterConfigHandlers(router *httprouter.Router, mgr *manager.Manager) error {
	return router.RegisterPath("", nil, httprequest.NewPathItem(
		"Configuration", "Get information about the filer endpoint",
	).Get(
		func(w http.ResponseWriter, r *http.Request) {
			backends := mgr.Backends()
			body := make(map[string]string, len(backends))
			for _, name := range backends {
				if b := mgr.Backend(name); b != nil {
					body[name] = b.URL().String()
				}
			}
			_ = httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema.BackendListResponse{
				Body: body,
			})
		},
		"Return Configuration",
		openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.BackendListResponse]()),
		openapi.WithDescription("Get information about the filer endpoint"),
	))
}

func RegisterListHandlers(router *httprouter.Router, mgr *manager.Manager) error {
	return router.RegisterPath("{name}", nil, httprequest.NewPathItem(
		"Objects", "Manage objects at the backend root",
	).Get(
		func(w http.ResponseWriter, r *http.Request) { _ = objectList(w, r, mgr) },
		"List objects",
		openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.ListObjectsResponse]()),
		openapi.WithDescription("List objects at the backend root"),
	).Post(
		func(w http.ResponseWriter, r *http.Request) { _ = objectUpload(w, r, mgr) },
		"Upload files",
		openapi.WithMultipartRequest(),
		openapi.WithDescription("Upload one or more files to the backend root using multipart/form-data (field name: \"file\", repeatable)"),
	).Delete(
		func(w http.ResponseWriter, r *http.Request) { _ = objectDeleteRoot(w, r, mgr) },
		"Delete objects",
		openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.DeleteObjectsResponse]()),
		openapi.WithDescription("Delete objects at the backend root"),
	))
}

func RegisterResourceHandlers(router *httprouter.Router, mgr *manager.Manager) error {
	return router.RegisterPath("{name}/{path...}", nil, httprequest.NewPathItem(
		"Object", "Manage a specific object",
	).Get(
		func(w http.ResponseWriter, r *http.Request) { _ = objectGet(w, r, mgr) },
		"Download object",
		openapi.WithDescription("Download an object"),
	).Head(
		func(w http.ResponseWriter, r *http.Request) { _ = objectHead(w, r, mgr) },
		"Get object metadata",
		openapi.WithDescription("Get object metadata without body"),
	).Put(
		func(w http.ResponseWriter, r *http.Request) { _ = objectPut(w, r, mgr) },
		"Create or replace object",
		openapi.WithJSONResponse(http.StatusCreated, jsonschema.MustFor[schema.Object]()),
		openapi.WithDescription("Create or replace an object"),
	).Post(
		func(w http.ResponseWriter, r *http.Request) { _ = objectUpload(w, r, mgr) },
		"Upload files",
		openapi.WithMultipartRequest(),
		openapi.WithDescription("Upload one or more files using multipart/form-data (field name: \"file\", repeatable)"),
	).Delete(
		func(w http.ResponseWriter, r *http.Request) { _ = objectDelete(w, r, mgr) },
		"Delete object",
		openapi.WithDescription("Delete an object or a set of objects (add ?recursive for bulk delete)"),
	))
}
