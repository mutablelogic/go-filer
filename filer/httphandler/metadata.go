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
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterMetadataHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Metadata", "Metadata Operations")

	return errors.Join(
		router.RegisterPath("metadata", nil, httprequest.NewPathItem("Metadata", "Manage metadata").
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetMetadata(w, r, manager)
				},
				"Extract metadata for a file",
				openapi.WithTags("Metadata"),
				openapi.WithMultipartRequest(jsonschema.MustFor[metadataRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.ObjectMeta]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

type fileReader struct {
	reader io.Reader
	name   string
}

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

type metadataRequest struct {
	File types.File `json:"file" help:"The file to extract metadata from" validate:"required"`
}

func GetMetadata(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req metadataRequest
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), nil)
	}
	if req.File.Body == nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(`missing or unreadable "file" form field`), nil)
	}
	defer req.File.Body.Close()

	reader := fileReader{reader: req.File.Body, name: req.File.Path}
	if resp, err := manager.GetMetadata(r.Context(), reader); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), nil)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}
}
