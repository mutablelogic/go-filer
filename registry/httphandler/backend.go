package httphandler

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	registry "github.com/mutablelogic/go-filer/registry"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// backendScheme is the scheme of a backend, of its path
type backendScheme struct {
	Scheme string `json:"scheme" help:"Scheme of the backend, such as file"`
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// iconMaxAge is how long a browser keeps a backend's icon, which only
// changes when the server does
const iconMaxAge = 24 * 60 * 60

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// backendHandlers registers the icons of the backends
func backendHandlers(reg *registry.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("backend/{scheme}/icon", jsonschema.MustFor[backendScheme](), func(path httprequest.PathItem) {
		path.Tag("Volumes")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			scheme := r.PathValue("scheme")
			backend, err := reg.Backend(scheme)
			if err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), scheme)
				return
			}
			icon := backend.Icon()
			if icon == nil {
				httpresponse.Error(w, httpresponse.ErrNotFound.Withf("the backend of scheme %q has no icon", scheme), scheme)
				return
			}
			writeIcon(w, r, icon)
		}, func(op httprequest.PathOperation) {
			op.Summary("Get Backend Icon")
			op.Description(documentation.Section(3, "GET /backend/{scheme}/icon").Body)
			op.Response(http.StatusOK, "image/*", "The backend's icon, such as an SVG image")
			op.Response(http.StatusNotModified, "", "The icon matches If-None-Match")
			op.ErrorResponse(http.StatusNotFound, "No backend of the scheme, or it has no icon")
		})
	})
}

// writeIcon writes the icon, of its media type, whose entity tag is of its
// data, which a browser keeps, or 304, when it matches If-None-Match, of the
// weak comparison of a GET, so W/"<etag>" matches too
func writeIcon(w http.ResponseWriter, r *http.Request, icon gofiler.Icon) {
	data := icon.Data()
	sum := sha256.Sum256(data)
	etag := `"` + hex.EncodeToString(sum[:8]) + `"`
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "public, max-age="+strconv.Itoa(iconMaxAge))
	for _, match := range strings.Split(r.Header.Get("If-None-Match"), ",") {
		if match = strings.TrimPrefix(strings.TrimSpace(match), "W/"); match == etag || match == "*" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	w.Header().Set("Content-Type", icon.Type())
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(data)
	}
}
