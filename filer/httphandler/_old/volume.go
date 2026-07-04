package httphandler

import (
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

type VolumePathParams struct {
	Name string `json:"name" help:"Volume name" required:""`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterVolumeHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Volumes", "Volume Operations")

	return errors.Join(
		router.RegisterPath("volume", nil, httprequest.NewPathItem("Volumes", "Manage volumes").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListVolumes(w, r, manager)
				},
				"List volumes",
				openapi.WithTags("Volumes"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.VolumeListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.VolumeList]()),
			).
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateVolume(w, r, manager)
				},
				"Create volume",
				openapi.WithTags("Volumes"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.VolumeCreate]()),
				openapi.WithJSONResponse(http.StatusCreated, jsonschema.MustFor[schema.Volume]()),
			),
		),
		router.RegisterPath("volume/{name}", nil, httprequest.NewPathItem("Volumes", "Manage a volume").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetVolume(w, r, manager, r.PathValue("name"))
				},
				"Get volume",
				openapi.WithTags("Volumes"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume]()),
			).
			Delete(
				func(w http.ResponseWriter, r *http.Request) {
					_ = DeleteVolume(w, r, manager, r.PathValue("name"))
				},
				"Delete volume",
				openapi.WithTags("Volumes"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume]()),
			).
			Patch(
				func(w http.ResponseWriter, r *http.Request) {
					_ = UpdateVolume(w, r, manager, r.PathValue("name"))
				},
				"Mount or unmount a volume, change parameters",
				openapi.WithTags("Volumes"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.VolumeMeta]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume]()),
			),
		),
		router.RegisterPath("volume/{name}/reindex", nil, httprequest.NewPathItem("Volumes", "Reindex objects in a volume").
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ReindexVolume(w, r, manager, r.PathValue("name"))
				},
				"Reindex a volume",
				openapi.WithTags("Volumes"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.ObjectListFilters]()),
				openapi.WithNoContentResponse(http.StatusNoContent, "Reindexing started"),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func GetVolume(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	if volume, err := manager.GetVolume(r.Context(), name); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
	}
}

func UpdateVolume(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	var meta schema.VolumeMeta
	if err := httprequest.Read(r, &meta); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if volume, err := manager.UpdateVolume(r.Context(), name, meta); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
	}
}

func ListVolumes(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.VolumeListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if resp, err := manager.ListVolumes(r.Context(), req); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}
}

func DeleteVolume(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	if volume, err := manager.DeleteVolume(r.Context(), name); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
	}
}

func CreateVolume(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var meta schema.VolumeCreate
	if err := httprequest.Read(r, &meta); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if url_, err := url.Parse(meta.URL); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if volume, err := manager.CreateVolume(r.Context(), url_, meta.VolumeMeta); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), meta.URL)
	} else {
		return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), volume)
	}
}

func ReindexVolume(w http.ResponseWriter, r *http.Request, manager *manager.Manager, volume string) error {
	var req schema.ObjectListFilters
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if err := manager.ReindexVolume(r.Context(), volume, req); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
	} else {
		return httpresponse.Empty(w, http.StatusNoContent)
	}
}
