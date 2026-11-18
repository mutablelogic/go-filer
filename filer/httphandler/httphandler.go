package httphandler

import (
	_ "embed"
	"errors"
	"net/http"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/filer"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed README.md
var readme []byte

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// RegisterHandlers registers the filer API, for the volumes of the registry,
// with the router. The changes to their objects are streamed by the
// broadcaster's owner, as go-server's /changes.
func RegisterHandlers(registry *filer.Registry, router *httprouter.Router) error {
	var documentation = openapi.ParseMarkdown(readme)
	router.Spec().AddTagGroup("Filer", "Volumes", "Objects", "Sign In")
	return errors.Join(
		volumeHandlers(registry, router, documentation),
		objectHandlers(registry, router, documentation),
		signInHandlers(registry, router, documentation),
	)
}

// httpError writes the error, as a HTTP error, with the detail, or, of a
// backend whose user needs to sign in, which is 401, with how they sign in
func httpError(w http.ResponseWriter, err error, detail ...any) error {
	if signIn := authclient.SignInOf(err); signIn != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), signIn)
	}
	return httpresponse.Error(w, gofiler.HTTPErr(err), detail...)
}
