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
	manager "github.com/mutablelogic/go-filer/filer/manager"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	image "github.com/mutablelogic/go-filer/metadata/image"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterArtworkHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Artwork", "Artwork Operations")

	return errors.Join(
		router.RegisterPath("artwork", nil, httprequest.NewPathItem("Artwork", "Create artwork").
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateArtwork(w, r, manager)
				},
				"Create artwork",
				openapi.WithTags("Artwork"),
				openapi.WithMultipartRequest(jsonschema.MustFor[schema.ArtworkUploadRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Artwork]()),
			),
		),
		router.RegisterPath("artwork/{etag}", jsonschema.MustFor[schema.ArtworkKey](), httprequest.NewPathItem("Artwork", "Get artwork").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetArtwork(w, r, manager, r.PathValue("etag"))
				},
				"Get artwork",
				openapi.WithTags("Artwork"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Artwork]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func CreateArtwork(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.ArtworkUploadRequest
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}

	// Create the artwork from the image
	image, err := image.CreateArtwork(req.Data.Body)
	if err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}

	// Insert the artwork into the database and return it
	if artwork, err := manager.CreateArtwork(r.Context(), types.Value(image), nil); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), artwork)
	}
}

func GetArtwork(w http.ResponseWriter, r *http.Request, manager *manager.Manager, etag string) error {
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
		return nil
	} else if err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err))
	}

	// Add the ETag header, modified since header and content type header
	// We cache the artwork for a long time since it is immutable
	w.Header().Set("Cache-Control", "public, max-age=2500000")
	w.Header().Set("ETag", string(artwork.ETag))
	w.Header().Set("Last-Modified", artwork.CreatedAt.Format(http.TimeFormat))
	w.Header().Set(types.ContentLengthHeader, fmt.Sprintf("%d", len(artwork.Data)))

	return httpresponse.Write(w, http.StatusOK, artwork.Type, func(w io.Writer) (int, error) {
		return w.Write(artwork.Data)
	})
}
