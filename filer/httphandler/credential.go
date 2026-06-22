package httphandler

import (
	"errors"
	"net/http"

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
// LIFECYCLE

func RegisterCredentialHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Credentials", "Credential Operations")

	return errors.Join(
		router.RegisterPath("credential", nil, httprequest.NewPathItem("Credentials", "Manage credentials").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListCredentials(w, r, manager)
				},
				"List encrypted credential keys",
				openapi.WithTags("Credentials"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.CredentialListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.CredentialList]()),
			).
			Put(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateCredential(w, r, manager)
				},
				"Create or update an encrypted credential",
				openapi.WithTags("Credentials"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.CredentialCreate]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Credential]()),
			),
		),
		router.RegisterPath("credential/{key}", jsonschema.MustFor[schema.CredentialKey](), httprequest.NewPathItem("Credentials", "Manage a credential").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetCredential(w, r, manager, r.PathValue("key"))
				},
				"Get an encrypted credential",
				openapi.WithTags("Credentials"),
				openapi.WithDescription(`The passphrase must be provided in the request body as text/plain content type. The response contains the credential in JSON format.`),
				openapi.WithRequest(types.ContentTypeTextPlain, jsonschema.MustFor[string]()),
				openapi.WithResponse(http.StatusOK, types.ContentTypeJSON, jsonschema.MustFor[[]byte](), "Credential in JSON format"),
			).
			Delete(
				func(w http.ResponseWriter, r *http.Request) {
					_ = DeleteCredential(w, r, manager, r.PathValue("key"))
				},
				"Delete an encrypted credential",
				openapi.WithTags("Credentials"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Credential]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func GetCredential(w http.ResponseWriter, r *http.Request, manager *manager.Manager, key string) error {
	// we expect text/plain content type with the passphrase in the body
	var passphrase string
	if err := httprequest.Read(r, &passphrase); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if resp, err := manager.GetCredential(r.Context(), schema.CredentialKey{Key: key}, passphrase); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), key)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}
}

func ListCredentials(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.CredentialListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if resp, err := manager.ListCredentials(r.Context(), req); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), req.String())
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}
}

func CreateCredential(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.CredentialCreate
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if resp, err := manager.CreateCredential(r.Context(), req); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), req.RedactedString())
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}
}

func DeleteCredential(w http.ResponseWriter, r *http.Request, manager *manager.Manager, key string) error {
	if resp, err := manager.DeleteCredential(r.Context(), schema.CredentialKey{Key: key}); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), key)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}
}
