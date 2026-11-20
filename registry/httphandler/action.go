package httphandler

import (
	"encoding/json"
	"io"
	"net/http"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	gofiler "github.com/mutablelogic/go-filer"
	registry "github.com/mutablelogic/go-filer/registry"
	schema "github.com/mutablelogic/go-filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// actionKey is the name of an action of a volume
type actionKey struct {
	Name   string `json:"name" help:"Volume name"`
	Action string `json:"action" help:"Action name, such as task_list"`
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// maxActionInput is the most bytes of an action's input
const maxActionInput = 1 << 20

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// actionHandlers registers the handlers of the actions of volumes
func actionHandlers(reg *registry.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	if err := router.Register("volume/{name}/action", jsonschema.MustFor[objectName](), func(path httprequest.PathItem) {
		path.Tag("Actions")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			name := r.PathValue("name")
			if list, err := reg.ListActions(r.Context(), name); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), name)
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), list)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("List Actions")
			op.Description(documentation.Section(3, "GET /volume/{name}/action").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.ActionList](), "Actions of the volume, in order of their names")
			op.ErrorResponse(http.StatusNotFound, "Volume doesn't exist")
			op.ErrorResponse(http.StatusServiceUnavailable, "Volume is unmounted, or offline")
		})
	}); err != nil {
		return err
	}
	return router.Register("volume/{name}/action/{action}", jsonschema.MustFor[actionKey](), func(path httprequest.PathItem) {
		path.Tag("Actions")

		// POST
		path.Post(func(w http.ResponseWriter, r *http.Request) {
			name, action := r.PathValue("name"), r.PathValue("action")
			input, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxActionInput))
			if err != nil {
				httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("input: %v", err), name)
				return
			} else if len(input) == 0 {
				input = []byte("{}")
			} else if !json.Valid(input) {
				httpresponse.Error(w, httpresponse.ErrBadRequest.With("input isn't JSON"), name)
				return
			}
			if object, err := reg.CallAction(r.Context(), name, action, input); err != nil {
				// A backend whose user needs to sign in again is 401, with
				// how they sign in
				if signIn := authclient.SignInOf(err); signIn != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), signIn)
				} else {
					httpresponse.Error(w, gofiler.HTTPErr(err), name)
				}
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), object)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Call Action")
			op.Description(documentation.Section(3, "POST /volume/{name}/action/{action}").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Object](), "Object the action created or changed")
			op.ErrorResponse(http.StatusBadRequest, "Input isn't JSON, or isn't valid for the action")
			op.ErrorResponse(http.StatusUnauthorized, "The volume's user needs to sign in, with how they sign in")
			op.ErrorResponse(http.StatusNotFound, "Volume, or action, doesn't exist")
			op.ErrorResponse(http.StatusServiceUnavailable, "Volume is unmounted, or offline")
		})
	})
}
