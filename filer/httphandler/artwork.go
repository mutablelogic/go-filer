package httphandler

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	image "github.com/mutablelogic/go-filer/_filer/metadata/image"
	manager "github.com/mutablelogic/go-filer/filer/manager"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ArtworkETag struct {
	ETag string `json:"etag" help:"Artwork ETag"`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// RegisterArtworkHandlers registers the artwork handlers
func RegisterArtworkHandlers(manager *manager.Filer, router *httprouter.Router) error {
	router.Spec().AddTag("Artwork", documentation.Section(2, "Artwork Operations").Body)
	return errors.Join(
		router.Register("artwork", nil, func(path httprequest.PathItem) {
			path.Tag("Artwork")

			// POST
			path.Post(func(w http.ResponseWriter, r *http.Request) {
				var req schema.ArtworkUploadRequest
				if err := httprequest.Read(r, &req); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
					return
				}

				// Create the artwork from the image
				artwork, _, err := image.CreateArtwork(req.Data.Body)
				if err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
					return
				}

				// Insert the artwork into the database and return it
				if result, err := manager.CreateArtwork(r.Context(), types.Value(artwork), nil); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err))
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), result)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Create Artwork")
				op.Description(documentation.Section(3, "POST /artwork").Body)
				op.RequestBody(jsonschema.MustFor[schema.ArtworkUploadRequest](), types.ContentTypeFormData)
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Artwork](), "Created (or existing) artwork")
				op.ErrorResponse(http.StatusBadRequest, "Missing or unreadable image data, or unsupported image format")
			})
		}),
		router.Register("artwork/{etag}", jsonschema.MustFor[ArtworkETag](), func(path httprequest.PathItem) {
			path.Tag("Artwork")

			// GET
			path.Get(func(w http.ResponseWriter, r *http.Request) {
				etag := r.PathValue("etag")

				// Cache control headers
				ifModifiedSince := r.Header.Get(schema.ContentIfModifiedSinceHeader)
				ifNoneMatch := r.Header.Get(schema.ContentIfNoneMatchHeader)

				ptrForTime := func(s string) *time.Time {
					if s == "" {
						return nil
					} else if t, err := http.ParseTime(s); err != nil {
						return nil
					} else {
						return types.Ptr(t)
					}
				}

				// Normalise an ETag value: strip quotes and weak indicator
				ptrForETag := func(s string) *string {
					s = strings.TrimPrefix(s, "W/")
					s = strings.Trim(s, `"`)
					if s == "" {
						return nil
					}
					return types.Ptr(s)
				}

				// Get the artwork from the database and return it
				artwork, err := manager.GetArtwork(r.Context(), schema.GetArtworkRequest{
					Key:             schema.ArtworkKey(etag),
					IfModifiedSince: ptrForTime(ifModifiedSince),
					IfNoneMatch:     ptrForETag(ifNoneMatch),
				})
				if errors.Is(err, gofiler.ErrNotModified) {
					w.WriteHeader(http.StatusNotModified)
					return
				} else if err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err))
					return
				}

				// Add the ETag header, modified since header and content type header
				// We cache the artwork for a long time since it is immutable
				w.Header().Set("Cache-Control", "public, max-age=2500000")
				w.Header().Set(types.ContentHashHeader, string(artwork.ETag))
				w.Header().Set(types.ContentModifiedHeader, artwork.CreatedAt.Format(http.TimeFormat))
				w.Header().Set(types.ContentLengthHeader, fmt.Sprintf("%d", len(artwork.Data)))

				httpresponse.Write(w, http.StatusOK, artwork.Type, func(w io.Writer) (int, error) {
					return w.Write(artwork.Data)
				})
			}, func(op httprequest.PathOperation) {
				op.Summary("Get Artwork")
				op.Description(documentation.Section(3, "GET /artwork/{etag}").Body)
				op.Response(http.StatusOK, "image/*", "Artwork image data")
				op.Response(http.StatusNotModified, "", "Artwork matches the conditional request headers")
				op.ErrorResponse(http.StatusNotFound, "Artwork not found")
			})
		}),
	)
}
