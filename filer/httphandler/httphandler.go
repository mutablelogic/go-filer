package httphandler

import (
	_ "embed"
	"errors"
	"net/http"
	"net/url"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	manager "github.com/mutablelogic/go-filer/filer/manager"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type VolumeName struct {
	Volume string `json:"volume" help:"Volume name"`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed README.md
var readme []byte
var documentation = openapi.ParseMarkdown(readme)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterHandlers(manager *manager.Filer, router *httprouter.Router) error {
	return errors.Join(
		RegisterVolumeHandlers(manager, router),
	)
}

/*
	router.Spec().AddTag("Objects", documentation.Section(2, "Object Operations").Body)
	router.Spec().AddTag("Search", documentation.Section(2, "Search Operations").Body)
	router.Spec().AddTag("Metadata", documentation.Section(2, "Metadata Operations").Body)
	router.Spec().AddTag("Artwork", documentation.Section(2, "Artwork Operations").Body)
	router.Spec().AddTag("LLM", documentation.Section(2, "Large Language Model (LLM) Operations").Body)

	return nil
}
*/

// RegisterVolumeHandlers registers the volume handlers
func RegisterVolumeHandlers(manager *manager.Filer, router *httprouter.Router) error {
	// VOLUMES
	router.Spec().AddTag("Volumes", documentation.Section(2, "Volume Operations").Body)
	return errors.Join(
		router.Register("volume", nil, func(path httprequest.PathItem) {
			path.Tag("Volumes")

			// GET
			path.Get(func(w http.ResponseWriter, r *http.Request) {
				var req schema.VolumeListRequest
				if err := httprequest.Query(r.URL.Query(), &req); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
				} else if resp, err := manager.ListVolumes(r.Context(), req); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("List Volumes")
				op.Description(documentation.Section(3, "GET /volume").Body)
				op.Query(jsonschema.MustFor[schema.VolumeListRequest]())
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.VolumeList](), "List of volumes")
				op.ErrorResponse(http.StatusBadRequest, "Invalid request")
			})

			// POST
			path.Post(func(w http.ResponseWriter, r *http.Request) {
				var meta schema.VolumeCreate
				if err := httprequest.Read(r, &meta); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
				} else if url_, err := url.Parse(meta.URL); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
				} else if volume, err := manager.CreateVolume(r.Context(), url_, meta.VolumeMeta); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), meta.URL)
				} else {
					httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), volume)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Create Volume")
				op.Description(documentation.Section(3, "POST /volume").Body)
				op.RequestBody(jsonschema.MustFor[schema.VolumeCreate]())
				op.JSONResponse(http.StatusCreated, jsonschema.MustFor[schema.Volume](), "Created volume")
				op.ErrorResponse(http.StatusBadRequest, "Invalid request")
			})
		}),
		router.Register("volume/{volume}", jsonschema.MustFor[VolumeName](), func(path httprequest.PathItem) {
			path.Tag("Volumes")

			// GET
			path.Get(func(w http.ResponseWriter, r *http.Request) {
				if volume, err := manager.GetVolume(r.Context(), r.PathValue("volume")); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("volume"))
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Get Volume")
				op.Description(documentation.Section(3, "GET /volume/{volume}").Body)
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume](), "Volume")
				op.ErrorResponse(http.StatusNotFound, "Volume not found")
			})

			// DELETE
			path.Delete(func(w http.ResponseWriter, r *http.Request) {
				if volume, err := manager.DeleteVolume(r.Context(), r.PathValue("volume")); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("volume"))
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Delete Volume")
				op.Description(documentation.Section(3, "DELETE /volume/{volume}").Body)
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume](), "Deleted volume")
				op.ErrorResponse(http.StatusNotFound, "Volume not found")
				op.ErrorResponse(http.StatusConflict, "Volume must be unmounted before it can be deleted")
			})

			// PATCH
			path.Patch(func(w http.ResponseWriter, r *http.Request) {
				var meta schema.VolumeMeta
				if err := httprequest.Read(r, &meta); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
				} else if volume, err := manager.UpdateVolume(r.Context(), r.PathValue("volume"), meta); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("volume"))
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Update Volume")
				op.Description(documentation.Section(3, "PATCH /volume/{volume}").Body)
				op.RequestBody(jsonschema.MustFor[schema.VolumeMeta]())
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume](), "Updated volume")
				op.ErrorResponse(http.StatusBadRequest, "Invalid request")
				op.ErrorResponse(http.StatusNotFound, "Volume not found")
			})
		}),
		router.Register("volume/{volume}/reindex", jsonschema.MustFor[VolumeName](), func(path httprequest.PathItem) {
			path.Tag("Volumes")

			// POST
			path.Post(func(w http.ResponseWriter, r *http.Request) {
				var req schema.ObjectListFilters
				if err := httprequest.Read(r, &req); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
				} else if err := manager.ReindexVolume(r.Context(), r.PathValue("volume"), req); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
				} else {
					httpresponse.Empty(w, http.StatusNoContent)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Reindex Volume")
				op.Description(documentation.Section(3, "POST /volume/{volume}/reindex").Body)
				op.RequestBody(jsonschema.MustFor[schema.ObjectListFilters]())
				op.Response(http.StatusNoContent, "", "Reindexing started")
				op.ErrorResponse(http.StatusBadRequest, "Invalid request")
				op.ErrorResponse(http.StatusServiceUnavailable, "Volume not mounted or not indexed")
			})
		}),
	)
}
