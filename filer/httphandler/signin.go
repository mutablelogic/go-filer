package httphandler

import (
	"net/http"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
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

func signInHandlers(registry *filer.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	router.Spec().AddTag("Sign In", documentation.Section(2, "Sign-In Operations").Body)
	return router.Register("signin", nil, func(path httprequest.PathItem) {
		path.Tag("Sign In")

		// POST
		path.Post(func(w http.ResponseWriter, r *http.Request) {
			var req authclient.SignInCode
			if err := httprequest.Read(r, &req); err != nil {
				httpresponse.Error(w, err)
			} else if response, err := registry.SignIn(r.Context(), req); err != nil {
				httpError(w, err, req.Credential)
			} else {
				httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), response)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Sign In")
			op.Description(documentation.Section(3, "POST /signin").Body)
			op.RequestBody(jsonschema.MustFor[authclient.SignInCode]())
			op.JSONResponse(http.StatusCreated, jsonschema.MustFor[schema.SignInResponse](), "The credential which is stored, and its scopes")
			op.ErrorResponse(http.StatusBadRequest, "Invalid request, a credential which isn't a sign-in's, or a redirect URI which isn't a loopback address's")
			op.ErrorResponse(http.StatusUnauthorized, "The code couldn't be exchanged")
			op.ErrorResponse(http.StatusServiceUnavailable, "There's no client, or provider of credentials")
		})
	})
}
