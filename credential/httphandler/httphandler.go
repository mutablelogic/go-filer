package httphandler

import (
	_ "embed"
	"encoding/json"
	"errors"
	"net/http"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	manager "github.com/mutablelogic/go-filer/credential/manager"
	schema "github.com/mutablelogic/go-filer/credential/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

//go:embed README.md
var readme []byte

type CredentialKey struct {
	Key string `json:"key" help:"Credential key"`
}

func RegisterCredentialHandlers(manager *manager.Manager, router *httprouter.Router) error {
	documentation := openapi.ParseMarkdown(readme)
	router.Spec().AddTag("Credentials", documentation.Section(1, "Credential Operations").Body)

	return errors.Join(
		router.Register("credential", nil, func(path httprequest.PathItem) {
			path.Tag("Credentials")

			// GET
			path.Get(func(w http.ResponseWriter, r *http.Request) {
				var req schema.CredentialListRequest
				if err := httprequest.Query(r.URL.Query(), &req); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
				} else if resp, err := manager.ListCredentials(r.Context(), req); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), req.String())
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("List Credentials")
				op.Description(documentation.Section(2, "GET /credential").Body)
				op.Query(jsonschema.MustFor[schema.CredentialListRequest]())
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.CredentialList](), "List of credentials")
				op.ErrorResponse(http.StatusBadRequest, "Invalid request")
				op.ErrorResponse(http.StatusServiceUnavailable, "No passphrase configured")
			})

			// PUT
			path.Put(func(w http.ResponseWriter, r *http.Request) {
				var req schema.CredentialCreate
				if err := httprequest.Read(r, &req); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
				} else if resp, err := manager.CreateCredential(r.Context(), req); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), req.RedactedString())
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Create or Update Credential")
				op.Description(documentation.Section(2, "PUT /credential").Body)
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Credential](), "Created credential")
				op.ErrorResponse(http.StatusBadRequest, "Invalid request")
				op.ErrorResponse(http.StatusConflict, "Credential already exists")
			})
		}),
		router.Register("credential/{key}/rotate", jsonschema.MustFor[CredentialKey](), func(path httprequest.PathItem) {
			path.Tag("Credentials")

			// POST
			path.Post(func(w http.ResponseWriter, r *http.Request) {
				if resp, err := manager.RotateCredential(r.Context(), schema.CredentialKey{Key: r.PathValue("key")}); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("key"))
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Rotate Credential")
				op.Description(documentation.Section(2, "POST /credential/{key}/rotate").Body)
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Credential](), "Rotated credential")
				op.ErrorResponse(http.StatusNotModified, "Credential already at latest passphrase version")
				op.ErrorResponse(http.StatusBadRequest, "Invalid request")
				op.ErrorResponse(http.StatusNotFound, "Credential not found")
				op.ErrorResponse(http.StatusServiceUnavailable, "No passphrase configured")
			})
		}),
		router.Register("credential/{key}", jsonschema.MustFor[CredentialKey](), func(path httprequest.PathItem) {
			path.Tag("Credentials")

			// GET - passphrase supplied as text/plain body
			path.Get(func(w http.ResponseWriter, r *http.Request) {
				var passphrase string
				if err := httprequest.Read(r, &passphrase); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
				} else if resp, err := manager.GetCredential(r.Context(), schema.CredentialKey{Key: r.PathValue("key")}, passphrase); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("key"))
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Get Credential")
				op.Description(documentation.Section(2, "GET /credential/{key}").Body)
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[json.RawMessage](), "Credential in JSON format")
				op.ErrorResponse(http.StatusBadRequest, "Invalid passphrase")
				op.ErrorResponse(http.StatusNotFound, "Credential not found")
			})

			// DELETE
			path.Delete(func(w http.ResponseWriter, r *http.Request) {
				if resp, err := manager.DeleteCredential(r.Context(), schema.CredentialKey{Key: r.PathValue("key")}); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("key"))
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Delete Credential")
				op.Description(documentation.Section(2, "DELETE /credential/{key}").Body)
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Credential](), "Deleted credential")
				op.ErrorResponse(http.StatusNotFound, "Credential not found")
			})
		}),
	)
}
