package handler

import (
	"context"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	// Packages
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	plugin "github.com/mutablelogic/go-filer"
	aws "github.com/mutablelogic/go-filer/pkg/aws"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ObjectCreate(w http.ResponseWriter, r *http.Request, client plugin.AWS, bucket string) error {
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
	if _, err := client.S3().HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: types.StringPtr(bucket),
	}); err != nil {
		return httpresponse.Error(w, aws.Err(err))
	}

	// Read the multipart form, and create the objects
	objects, err := uploadParts(ctx, multipart.NewReader(r.Body, boundary), client, bucket)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	// Return the objects
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), schema.ObjectList{
		Count: uint64(len(objects)),
		Body:  objects,
	})
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func uploadParts(ctx context.Context, reader *multipart.Reader, client plugin.AWS, bucket string) ([]schema.Object, error) {
	var objects []schema.Object

	// Read parts
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			// Return uploaded objects
			return objects, nil
		} else if err != nil {
			// Rollback, return an error
			return nil, errors.Join(err, deleteObjects(ctx, client, bucket, objects))
		}

		if object, err := uploadPart(context.Background(), part, client, bucket); err != nil {
			// Rollback, return an error
			return nil, errors.Join(err, deleteObjects(ctx, client, bucket, objects))
		} else {
			objects = append(objects, *object)
		}
	}
}

func deleteObjects(ctx context.Context, client plugin.AWS, bucket string, objects []schema.Object) error {
	var result error
	for _, object := range objects {
		if err := client.DeleteObject(ctx, bucket, object.Key); err != nil {
			result = errors.Join(result, err)
		}
	}

	// Return any errors
	return result
}

func uploadPart(ctx context.Context, part *multipart.Part, client plugin.AWS, bucket string) (*schema.Object, error) {
	defer part.Close()

	// Get the content type and filename
	contentType, filename, params, err := parseContentDisposition(part)
	if err != nil {
		return nil, err
	}
	var contentLength int64
	if params.Has(types.ContentLengthHeader) {
		if length, err := strconv.ParseInt(params.Get(types.ContentLengthHeader), 10, 64); err != nil {
			return nil, httpresponse.ErrBadRequest.With("invalid content length")
		} else {
			contentLength = length
		}
	}

	// We always remove the first '/' from the filename
	if strings.HasPrefix(filename, schema.PathSeparator) {
		filename = filename[1:]
	}
	if strings.HasSuffix(filename, schema.PathSeparator) {
		return nil, httpresponse.ErrBadRequest.With("object key cannot end with a separator")
	}

	// Insert the object into S3
	object, err := client.PutObject(ctx, bucket, filename, part,
		aws.WithContentType(contentType),
		aws.WithContentLength(contentLength),
		aws.WithMeta(params),
	)
	if err != nil {
		return nil, err
	}

	// Retrieve the object
	object, meta, err := client.GetObjectMeta(ctx, bucket, types.PtrString(object.Key))
	if err != nil {
		return nil, err
	}

	// Return the object uploaded
	return schema.ObjectFromAWS(object, bucket, meta), nil
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
