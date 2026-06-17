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
	Volume string `json:"volume"`
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
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

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
