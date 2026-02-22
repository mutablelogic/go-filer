package httphandler

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// objectList handles GET requests to list objects at a URL path.
// The path is constructed from the request path: /api/filer/{scheme}/{host}{path...}
// Examples:
//
//	/api/filer/file/media        → file://media/
//	/api/filer/file/media/podcasts → file://media/podcasts
func objectList(w http.ResponseWriter, r *http.Request, manager *filer.Manager, prefix string) error {
	// Parse query parameters into request struct
	var request schema.ListObjectsRequest
	if err := httprequest.Query(r.URL.Query(), &request); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}

	// Set the URL with query parameters
	u := url.URL{
		Scheme:   r.PathValue("scheme"),
		Host:     r.PathValue("host"),
		Path:     r.PathValue("path"),
		RawQuery: r.URL.RawQuery,
	}
	request.URL = u.String()

	// List objects
	response, err := manager.ListObjects(r.Context(), request)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

// objectPut handles PUT requests to create or replace a file at a URL path.
// The path is constructed from the request path: /api/filer/{scheme}/{host}{path...}
func objectPut(w http.ResponseWriter, r *http.Request, manager *filer.Manager, prefix string) error {
	scheme := r.PathValue("scheme")
	host := r.PathValue("host")
	pathValue := r.PathValue("path")

	// Build the target URL
	u := url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     pathValue,
		RawQuery: r.URL.RawQuery,
	}

	// Create the object
	obj, err := manager.CreateObject(r.Context(), schema.CreateObjectRequest{
		URL:  u.String(),
		Body: r.Body,
	})
	if err != nil {
		// Check if the error is due to trying to overwrite a directory
		if strings.Contains(err.Error(), "is a directory") {
			return httpresponse.Error(w, httpresponse.ErrBadRequest.With("cannot overwrite directory with file"))
		}
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), obj)
}

// objectDelete handles DELETE requests to delete a file at a URL path.
// The path is constructed from the request path: /api/filer/{scheme}/{host}{path...}
func objectDelete(w http.ResponseWriter, r *http.Request, manager *filer.Manager, prefix string) error {
	scheme := r.PathValue("scheme")
	host := r.PathValue("host")
	pathValue := r.PathValue("path")

	// Build the target URL
	u := url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     pathValue,
		RawQuery: r.URL.RawQuery,
	}

	// Delete the object
	obj, err := manager.DeleteObject(r.Context(), schema.DeleteObjectRequest{
		URL: u.String(),
	})
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), obj)
}

// objectHead handles HEAD requests to get file metadata without downloading the body.
// It sets Content-Type, Content-Length, Last-Modified, and X-Object-Meta headers.
func objectHead(w http.ResponseWriter, r *http.Request, manager *filer.Manager, prefix string) error {
	scheme := r.PathValue("scheme")
	host := r.PathValue("host")
	pathValue := r.PathValue("path")

	// Build the target URL
	u := url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     pathValue,
		RawQuery: r.URL.RawQuery,
	}

	// Read the object metadata (without reading the body)
	reader, obj, err := manager.ReadObject(r.Context(), schema.ReadObjectRequest{
		URL: u.String(),
	})
	if err != nil {
		return httpresponse.Error(w, err)
	}
	defer reader.Close()

	// Detect content type from file extension
	contentType := mime.TypeByExtension(filepath.Ext(pathValue))
	if contentType == "" {
		contentType = types.ContentTypeBinary
	}

	// Set headers
	w.Header().Set(types.ContentTypeHeader, contentType)
	if obj.Size >= 0 {
		w.Header().Set(types.ContentLengthHeader, fmt.Sprint(obj.Size))
	}
	w.Header().Set(types.ContentModifiedHeader, obj.ModTime.Format(http.TimeFormat))

	// Add object metadata as JSON in custom header
	if metaJSON, err := json.Marshal(obj); err == nil {
		w.Header().Set(schema.ObjectMetaHeader, string(metaJSON))
	}

	w.WriteHeader(http.StatusOK)

	// No body for HEAD request
	return nil
}

// objectGet handles GET requests to download a single file.
// It detects the content type and includes object metadata in headers.
func objectGet(w http.ResponseWriter, r *http.Request, manager *filer.Manager, prefix string) error {
	scheme := r.PathValue("scheme")
	host := r.PathValue("host")
	pathValue := r.PathValue("path")

	// Build the target URL
	u := url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     pathValue,
		RawQuery: r.URL.RawQuery,
	}

	// Read the object
	reader, obj, err := manager.ReadObject(r.Context(), schema.ReadObjectRequest{
		URL: u.String(),
	})
	if err != nil {
		return httpresponse.Error(w, err)
	}
	defer reader.Close()

	// Detect content type by reading first 512 bytes
	buffer := make([]byte, 512)
	n, err := io.ReadFull(reader, buffer)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return httpresponse.Error(w, err)
	}

	// Detect content type from the buffer
	contentType := http.DetectContentType(buffer[:n])

	// If detection failed or returned generic type, try file extension
	if contentType == types.ContentTypeBinary {
		if extType := mime.TypeByExtension(filepath.Ext(pathValue)); extType != "" {
			contentType = extType
		}
	}

	// Set headers
	w.Header().Set(types.ContentTypeHeader, contentType)
	if obj.Size >= 0 {
		w.Header().Set(types.ContentLengthHeader, fmt.Sprint(obj.Size))
	}
	w.Header().Set(types.ContentModifiedHeader, obj.ModTime.Format(http.TimeFormat))

	// Add object metadata as JSON in custom header
	if metaJSON, err := json.Marshal(obj); err == nil {
		w.Header().Set(schema.ObjectMetaHeader, string(metaJSON))
	}

	w.WriteHeader(http.StatusOK)

	// Write the buffered bytes first
	if n > 0 {
		if _, err := w.Write(buffer[:n]); err != nil {
			return err
		}
	}

	// Stream the rest of the file
	if _, err := io.Copy(w, reader); err != nil {
		return err
	}

	// Return success
	return nil
}
