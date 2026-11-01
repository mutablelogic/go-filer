package httphandler

import (
	"errors"
	"net/http"
	"net/url"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func volumeHandlers(registry *filer.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	router.Spec().AddTag("Volumes", documentation.Section(2, "Volume Operations").Body)
	return errors.Join(
		volumeListHandlers(registry, router, documentation),
		volumeGetDeleteHandlers(registry, router, documentation),
	)
}

func volumeGetDeleteHandlers(registry *filer.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("volume/{name}", jsonschema.MustFor[objectName](), func(path httprequest.PathItem) {
		path.Tag("Volumes")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			name := r.PathValue("name")
			if volume, err := registry.Volume(name); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), name)
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Get Volume")
			op.Description(documentation.Section(3, "GET /volume/{name}").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume](), "Volume, with the URL of its backend")
			op.ErrorResponse(http.StatusNotFound, "Volume isn't mounted")
		})

		// PATCH
		path.Patch(func(w http.ResponseWriter, r *http.Request) {
			name := r.PathValue("name")
			var patch schema.VolumePatch
			if err := httprequest.Read(r, &patch); err != nil {
				httpresponse.Error(w, err)
			} else if values, err := patch.Values(); err != nil {
				httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("%v", err), name)
			} else if len(values) == 0 {
				httpresponse.Error(w, httpresponse.ErrBadRequest.With("no options to update"), name)
			} else if volume, err := registry.Update(r.Context(), name, values); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), name)
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Update Volume")
			op.Description(documentation.Section(3, "PATCH /volume/{name}").Body)
			op.RequestBody(jsonschema.MustFor[schema.VolumePatch]())
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume](), "Updated volume, with the URL of its backend")
			op.ErrorResponse(http.StatusBadRequest, "Invalid request body, no options, or an option which is unknown or invalid")
			op.ErrorResponse(http.StatusNotFound, "Volume isn't mounted")
			op.ErrorResponse(http.StatusServiceUnavailable, "Registry is closed")
		})

		// DELETE
		path.Delete(func(w http.ResponseWriter, r *http.Request) {
			name := r.PathValue("name")
			if volume, err := registry.Volume(name); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), name)
			} else if err := registry.Del(r.Context(), name); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), name)
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Delete Volume")
			op.Description(documentation.Section(3, "DELETE /volume/{name}").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume](), "Deleted volume, with the URL of its backend")
			op.ErrorResponse(http.StatusNotFound, "Volume isn't mounted")
		})
	})
}

func volumeListHandlers(registry *filer.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("volume", nil, func(path httprequest.PathItem) {
		path.Tag("Volumes")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema.VolumeList{
				Body:    registry.Volumes(),
				Schemes: registry.Schemes(),
			})
		}, func(op httprequest.PathOperation) {
			op.Summary("List Volumes")
			op.Description(documentation.Section(3, "GET /volume").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.VolumeList](), "Volumes, and the schemes of their URLs")
		})

		// POST
		path.Post(func(w http.ResponseWriter, r *http.Request) {
			var req schema.Volume
			if err := httprequest.Read(r, &req); err != nil {
				httpresponse.Error(w, err)
			} else if u, err := url.Parse(req.URL); err != nil || req.URL == "" {
				httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("invalid url: %q", req.URL))
			} else if _, err := registry.Add(r.Context(), req.Name, u); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), req.Name)
			} else if volume, err := registry.Volume(req.Name); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), req.Name)
			} else {
				httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), volume)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Create Volume")
			op.Description(documentation.Section(3, "POST /volume").Body)
			op.RequestBody(jsonschema.MustFor[schema.Volume]())
			op.JSONResponse(http.StatusCreated, jsonschema.MustFor[schema.Volume](), "Created volume, with the URL of its backend")
			op.ErrorResponse(http.StatusBadRequest, "Invalid request body, name or URL, such as an unsupported scheme, a host which isn't the name, a path which isn't an absolute path of a directory, or an unknown option")
			op.ErrorResponse(http.StatusConflict, "A volume with the name exists, or is being added")
			op.ErrorResponse(http.StatusServiceUnavailable, "Registry is closed")
		})
	})
}
