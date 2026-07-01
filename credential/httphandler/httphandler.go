package httphandler

import (
	_ "embed"
	"errors"

	// Packages
	manager "github.com/mutablelogic/go-filer/filer/manager"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	"github.com/mutablelogic/go-server/pkg/jsonschema"
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
		router.Register("credential", nil, func(path httprequest.PathItem) error {
			path.Tag("Credentials")

			// GET
			path.Get(func(w httprequest.ResponseWriter, r *httprequest.Request) {
				// TODO
			}, func(op openapi.Operation) {
				// TODO
			})

			// PUT
			path.Put(func(w httprequest.ResponseWriter, r *httprequest.Request) {
				// TODO
			}, func(op openapi.Operation) {
				// TODO
			})

			return nil
		}),
		router.Register("credential/{key}", jsonschema.MustFor[CredentialKey](), func(path httprequest.PathItem) error {
			path.Tag("Credentials")
			path.Get()
			return nil
		}),
	)
}
