package httphandler

import (
	"errors"
	"net/http"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	manager "github.com/mutablelogic/go-filer/filer/manager"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterCredentialHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Credentials", "Credential Operations")

	return errors.Join(
		router.RegisterPath("credential", nil, httprequest.NewPathItem("Credentials", "Manage credentials").
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateCredential(w, r, manager, r.PathValue("key"))
				},
				"Create or update an encrypted credential",
				openapi.WithTags("Credentials"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Credential]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func CreateCredential(w http.ResponseWriter, r *http.Request, manager *manager.Manager, key string) error {
	return gofiler.ErrNotImplemented
}
