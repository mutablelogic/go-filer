package httphandler

import (
	"errors"
	"net/http"
	"net/url"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
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
			if volume, err := registry.GetVolume(r.Context(), name); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), name)
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Get Volume")
			op.Description(documentation.Section(3, "GET /volume/{name}").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume](), "Volume, with the URL of its backend")
			op.ErrorResponse(http.StatusNotFound, "Volume doesn't exist")
		})

		// PATCH
		path.Patch(func(w http.ResponseWriter, r *http.Request) {
			name := r.PathValue("name")
			var patch schema.VolumePatch
			if err := httprequest.Read(r, &patch); err != nil {
				httpresponse.Error(w, err)
			} else if values, err := patch.Values(); err != nil {
				httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("%v", err), name)
			} else if volume, err := updateVolume(r, registry, name, values); err != nil {
				// A backend whose user needs to sign in again is 401, with
				// how they sign in
				if signIn := authclient.SignInOf(err); signIn != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), signIn)
				} else {
					httpresponse.Error(w, gofiler.HTTPErr(err), name)
				}
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Update Volume")
			op.Description(documentation.Section(3, "PATCH /volume/{name}").Body)
			op.RequestBody(jsonschema.MustFor[schema.VolumePatch]())
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume](), "Updated volume, with the URL of its backend")
			op.ErrorResponse(http.StatusBadRequest, "Invalid request body, or an option which is unknown or invalid")
			op.ErrorResponse(http.StatusUnauthorized, "The volume's user needs to sign in, with how they sign in")
			op.ErrorResponse(http.StatusNotFound, "Volume doesn't exist")
		})

		// DELETE
		path.Delete(func(w http.ResponseWriter, r *http.Request) {
			name := r.PathValue("name")
			if volume, err := registry.DeleteVolume(r.Context(), name); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), name)
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), volume)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Delete Volume")
			op.Description(documentation.Section(3, "DELETE /volume/{name}").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Volume](), "Deleted volume, with the URL of its backend")
			op.ErrorResponse(http.StatusNotFound, "Volume doesn't exist")
		})
	})
}

func volumeListHandlers(registry *filer.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("volume", nil, func(path httprequest.PathItem) {
		path.Tag("Volumes")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			if volumes, err := registry.ListVolumes(r.Context()); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err))
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema.VolumeList{
					Body:    volumes,
					Schemes: registry.Schemes(),
				})
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("List Volumes")
			op.Description(documentation.Section(3, "GET /volume").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.VolumeList](), "Volumes, and the schemes of their URLs")
		})

		// POST
		path.Post(func(w http.ResponseWriter, r *http.Request) {
			var req schema.VolumeCreateRequest
			if err := httprequest.Read(r, &req); err != nil {
				httpresponse.Error(w, err)
			} else if _, err := url.Parse(req.URL); err != nil || req.URL == "" {
				httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("invalid url: %q", req.URL))
			} else if volume, err := registry.CreateVolume(r.Context(), req.Meta()); err != nil {
				// A backend whose user needs to sign in is 401, with how
				// they sign in
				if signIn := authclient.SignInOf(err); signIn != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), signIn)
				} else {
					httpresponse.Error(w, gofiler.HTTPErr(err), req.Name)
				}
			} else {
				httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), volume)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Create Volume")
			op.Description(documentation.Section(3, "POST /volume").Body)
			op.RequestBody(jsonschema.MustFor[schema.VolumeCreateRequest]())
			op.JSONResponse(http.StatusCreated, jsonschema.MustFor[schema.Volume](), "Created volume, with the URL of its backend")
			op.ErrorResponse(http.StatusBadRequest, "Invalid request body, name or URL, such as an unsupported scheme, a name which isn't the URL's, a path which isn't an absolute path of a directory, a bucket in another region than its region option, or an unknown option")
			op.ErrorResponse(http.StatusForbidden, "The bucket can't be accessed, with the credentials, or there are none")
			op.ErrorResponse(http.StatusNotFound, "The bucket doesn't exist")
			op.ErrorResponse(http.StatusConflict, "A volume with the name exists")
		})
	})
}

// updateVolume patches the options of a stored volume's URL, and updates it,
// keeping whether it's enabled
func updateVolume(r *http.Request, registry *filer.Registry, name string, patch url.Values) (*schema.Volume, error) {
	volume, err := registry.GetVolume(r.Context(), name)
	if err != nil {
		return nil, err
	}
	u, err := filer.PatchURL(volume.URL, patch)
	if err != nil {
		return nil, err
	}
	// It's enabled when it's stored enabled, which it is when it's offline,
	// which is Enabled false, with why
	return registry.UpdateVolume(r.Context(), schema.VolumeMeta{Name: name, URL: u, Enabled: volume.Enabled || volume.Error != ""})
}
