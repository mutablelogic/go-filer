package httphandler

import (
	"bytes"
	"net/http"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	registry "github.com/mutablelogic/go-filer/registry"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// artworkHash is the hash of artwork, of its path
type artworkHash struct {
	Hash string `json:"hash" help:"The SHA-256 hash of the artwork, in hex, of an object's artwork"`
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// artworkMaxAge is how long a browser keeps artwork, which never changes,
// as its hash is of its context and data
const artworkMaxAge = 365 * 24 * time.Hour

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// artworkHandlers registers the artwork, which objects link to, by its hash
func artworkHandlers(reg *registry.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("artwork/{hash}", jsonschema.MustFor[artworkHash](), func(path httprequest.PathItem) {
		path.Tag("Objects")

		// GET, and HEAD, of its headers alone
		serve := func(w http.ResponseWriter, r *http.Request) {
			hash := r.PathValue("hash")

			// Its headers, without its data, which isn't read when the
			// browser has it, or of a HEAD request
			artwork, err := reg.GetArtwork(r.Context(), hash, false)
			if err != nil {
				_ = httpresponse.Error(w, gofiler.HTTPErr(err), hash)
				return
			}
			binary := httpresponse.Binary{
				ContentType:  artwork.Type,
				Size:         types.Ptr(int64(artwork.Size)),
				ETag:         artwork.Hash,
				Disposition:  httpresponse.DispositionInline,
				CacheControl: httpresponse.CacheControl{MaxAge: artworkMaxAge, Immutable: true},
			}
			if r.Method == http.MethodHead || binary.NotModified(r) {
				if err := binary.Write(w, r, nil); err != nil {
					_ = httpresponse.Error(w, gofiler.HTTPErr(err), hash)
				}
				return
			}

			// Its data
			if artwork, err = reg.GetArtwork(r.Context(), hash, true); err != nil {
				_ = httpresponse.Error(w, gofiler.HTTPErr(err), hash)
				return
			}
			if err := binary.Write(w, r, bytes.NewReader(artwork.Data)); err != nil {
				_ = httpresponse.Error(w, gofiler.HTTPErr(err), hash)
			}
		}
		path.Get(serve, func(op httprequest.PathOperation) {
			op.Summary("Get Artwork")
			op.Description(documentation.Section(3, "GET /artwork/{hash}").Body)
			op.Response(http.StatusOK, "image/*", "The artwork's image")
			op.Response(http.StatusNotModified, "", "The artwork matches If-None-Match")
			op.ErrorResponse(http.StatusBadRequest, "The hash isn't one")
			op.ErrorResponse(http.StatusNotFound, "No artwork of the hash")
		})
		path.Head(serve, func(op httprequest.PathOperation) {
			op.Summary("Get Artwork Headers")
			op.Description(documentation.Section(3, "HEAD /artwork/{hash}").Body)
			op.Response(http.StatusOK, "", "The artwork's headers")
			op.Response(http.StatusNotModified, "", "The artwork matches If-None-Match")
			op.ErrorResponse(http.StatusBadRequest, "The hash isn't one")
			op.ErrorResponse(http.StatusNotFound, "No artwork of the hash")
		})
	})
}
