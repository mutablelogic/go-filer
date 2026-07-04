package httphandler

import (
	"errors"
	"io"
	"net/http"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
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

type metadataRequest struct {
	File types.File `json:"file" help:"The file to extract metadata from" validate:"required"`
}

type fileReader struct {
	reader io.Reader
	name   string
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// RegisterMetadataHandlers registers the metadata handlers
func RegisterMetadataHandlers(manager *manager.Filer, router *httprouter.Router) error {
	router.Spec().AddTag("Metadata", documentation.Section(2, "Metadata Operations").Body)
	return errors.Join(
		router.Register("metadata", nil, func(path httprequest.PathItem) {
			path.Tag("Metadata")

			// POST
			path.Post(func(w http.ResponseWriter, r *http.Request) {
				var req metadataRequest
				if err := httprequest.Read(r, &req); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err))
					return
				}
				if req.File.Body == nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(`missing or unreadable "file" form field`))
					return
				}
				defer req.File.Body.Close()

				reader := fileReader{reader: req.File.Body, name: req.File.Path}
				if resp, err := manager.GetMetadata(r.Context(), reader); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err))
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Extract Metadata")
				op.Description(documentation.Section(3, "POST /metadata").Body)
				op.RequestBody(jsonschema.MustFor[metadataRequest](), types.ContentTypeFormData)
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.ObjectMeta](), "Extracted metadata")
				op.ErrorResponse(http.StatusBadRequest, "Missing or unreadable file, or unrecognised content")
			})
		}),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (r fileReader) Read(p []byte) (int, error) {
	if r.reader == nil {
		return 0, io.ErrUnexpectedEOF
	}
	return r.reader.Read(p)
}

func (r fileReader) Name() string {
	return r.name
}

func (r fileReader) Seek(offset int64, whence int) (int64, error) {
	if s, ok := r.reader.(io.Seeker); ok {
		return s.Seek(offset, whence)
	}
	return 0, errors.New("not seekable")
}
