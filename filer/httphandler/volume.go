package httphandler

import (
	"errors"
	"net/http"
	"net/url"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	"github.com/mutablelogic/go-filer/filer/manager"
	"github.com/mutablelogic/go-filer/filer/schema"
	"github.com/mutablelogic/go-server/pkg/httprequest"
	"github.com/mutablelogic/go-server/pkg/httpresponse"
	"github.com/mutablelogic/go-server/pkg/httprouter"
	"github.com/mutablelogic/go-server/pkg/jsonschema"
	"github.com/mutablelogic/go-server/pkg/openapi"
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
