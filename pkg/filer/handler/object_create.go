package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func objectCreate(w http.ResponseWriter, r *http.Request, filer filer.Filer, bucket string) error {
	ctx := r.Context()

	// Parse the body
	mediaType, params, err := mime.ParseMediaType(r.Header.Get(types.ContentTypeHeader))
	if err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest, err.Error())
	} else if mediaType != types.ContentTypeFormData {
		return httpresponse.Error(w, httpresponse.ErrBadRequest, "expected "+types.ContentTypeFormData)
	}

	// Get the boundary
	boundary, exists := params["boundary"]
	if !exists {
		return httpresponse.Error(w, httpresponse.ErrBadRequest, "missing boundary")
	}

	// Check that the bucket exists
	if _, err := filer.GetBucket(ctx, bucket); err != nil {
		return err
	}

	// Read the multipart form, and create the objects
	objects, err := uploadParts(ctx, multipart.NewReader(r.Body, boundary), filer, bucket)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	// Return the objects
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), schema.ObjectList{
		Body: objects,
	})
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func uploadParts(ctx context.Context, reader *multipart.Reader, filer filer.Filer, bucket string) ([]*schema.Object, error) {
	var objects []*schema.Object

	// Read parts
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			// Return uploaded objects
			return objects, nil
		} else if err != nil {
			// Rollback, return an error
			return nil, errors.Join(err, deleteObjects(ctx, filer, bucket, objects))
		}

		if object, err := uploadPart(context.Background(), part, filer, bucket); err != nil {
			fmt.Println("Error uploading part:", err)
			// Rollback, return an error
			return nil, errors.Join(err, deleteObjects(ctx, filer, bucket, objects))
		} else {
			objects = append(objects, object)
		}
	}
}

func deleteObjects(ctx context.Context, filer filer.Filer, bucket string, objects []*schema.Object) error {
	var result error
	for _, object := range objects {
		if _, err := filer.DeleteObject(ctx, bucket, object.Key); err != nil {
			result = errors.Join(result, err)
		}
	}

	// Return any errors
	return result
}

func uploadPart(ctx context.Context, part *multipart.Part, client filer.Filer, bucket string) (*schema.Object, error) {
	defer part.Close()

	// Get the content type and filename
	contentType, filename, meta, err := parseContentDisposition(part)
	if err != nil {
		return nil, err
	}
	var contentLength int64
	if meta.Has(types.ContentLengthHeader) {
		if length, err := strconv.ParseInt(meta.Get(types.ContentLengthHeader), 10, 64); err != nil {
			return nil, httpresponse.ErrBadRequest.With("invalid content length")
		} else {
			contentLength = length
		}
	}

	// Insert the object into S3
	return client.PutObject(ctx, bucket, filename, part, filer.WithContentType(contentType), filer.WithContentLength(contentLength), filer.WithMeta(meta))
}

// Returns the content type, filename and metadata
func parseContentDisposition(part *multipart.Part) (string, string, url.Values, error) {
	_, params, err := mime.ParseMediaType(part.Header.Get(types.ContentDispositonHeader))
	if err != nil {
		return "", "", nil, err
	}
	contentType := part.Header.Get(types.ContentTypeHeader)
	if contentType == "" {
		contentType = types.ContentTypeBinary
	}
	filename, ok := params["filename"]
	if !ok {
		return "", "", nil, httpresponse.ErrBadRequest.With("missing filename")
	} else {
		filename = strings.TrimSpace(filename)
	}
	if filename == "" {
		return "", "", nil, httpresponse.ErrBadRequest.With("missing filename")
	}
	meta := make(url.Values, 20)
	for key, value := range params {
		meta[key] = []string{value}
	}
	for key, value := range part.Header {
		meta[key] = value
	}
	return contentType, filename, meta, nil
}
