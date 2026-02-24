package httphandler

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"

	// Packages
	manager "github.com/mutablelogic/go-filer/manager"
	schema "github.com/mutablelogic/go-filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	openapi "github.com/mutablelogic/go-server/pkg/openapi/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// HANDLER FUNCTIONS

// Path: /{name}
// GET lists objects at the backend root.
func ObjectListHandler(mgr *manager.Manager) (string, http.HandlerFunc, *openapi.PathItem) {
	return "/{name}", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				_ = objectList(w, r, mgr)
			default:
				_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
			}
		}, types.Ptr(openapi.PathItem{
			Get: &openapi.Operation{
				Description: "List objects at the backend root",
			},
		})
}

// Path: /{name}/{path...}
// GET downloads a file, HEAD returns metadata, PUT creates/replaces, DELETE removes.
func ObjectHandler(mgr *manager.Manager) (string, http.HandlerFunc, *openapi.PathItem) {
	return "/{name}/{path...}", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				_ = objectGet(w, r, mgr)
			case http.MethodHead:
				_ = objectHead(w, r, mgr)
			case http.MethodPut:
				_ = objectPut(w, r, mgr)
			case http.MethodDelete:
				_ = objectDelete(w, r, mgr)
			default:
				_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
			}
		}, types.Ptr(openapi.PathItem{
			Get: &openapi.Operation{
				Description: "Download a file",
			},
			Head: &openapi.Operation{
				Description: "Get file metadata without body",
			},
			Put: &openapi.Operation{
				Description: "Create or replace a file",
			},
			Delete: &openapi.Operation{
				Description: "Delete a file or series of files",
			},
		})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func objectList(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	var request schema.ListObjectsRequest
	if err := httprequest.Query(r.URL.Query(), &request); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}

	request.Name = r.PathValue("name")

	response, err := mgr.ListObjects(r.Context(), request)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func objectPut(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	req := schema.CreateObjectRequest{
		Name: r.PathValue("name"),
		Path: "/" + r.PathValue("path"),
		Body: r.Body,
	}

	// Forward Content-Type if provided
	if ct := r.Header.Get("Content-Type"); ct != "" {
		req.ContentType = ct
	}

	// Forward Last-Modified if provided
	if lm := r.Header.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			req.ModTime = t
		}
	}

	// Forward X-Meta-{key} headers as user-defined metadata (lowercased for S3 compatibility)
	for key, vals := range r.Header {
		if after, ok := strings.CutPrefix(key, schema.ObjectMetaKeyPrefix); ok && len(vals) > 0 {
			if req.Meta == nil {
				req.Meta = make(schema.ObjectMeta)
			}
			req.Meta[strings.ToLower(after)] = vals[0]
		}
	}

	obj, err := mgr.CreateObject(r.Context(), req)
	if err != nil {
		if strings.Contains(err.Error(), "is a directory") || strings.Contains(err.Error(), "file exists") {
			return httpresponse.Error(w, httpresponse.ErrBadRequest.With("cannot overwrite directory with file"))
		}
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), obj)
}

func objectDelete(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	var request schema.DeleteObjectsRequest
	if err := httprequest.Query(r.URL.Query(), &request); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	request.Name = r.PathValue("name")
	request.Path = "/" + r.PathValue("path")

	if request.Recursive {
		resp, err := mgr.DeleteObjects(r.Context(), request)
		if err != nil {
			return httpresponse.Error(w, err)
		}
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}

	obj, err := mgr.DeleteObject(r.Context(), schema.DeleteObjectRequest{
		Name: request.Name,
		Path: request.Path,
	})
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), obj)
}

func objectHead(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	pathValue := r.PathValue("path")
	obj, err := mgr.GetObject(r.Context(), schema.GetObjectRequest{
		Name: r.PathValue("name"),
		Path: "/" + pathValue,
	})
	if err != nil {
		return httpresponse.Error(w, err)
	}

	contentType := mime.TypeByExtension(filepath.Ext(pathValue))
	if contentType == "" {
		contentType = types.ContentTypeBinary
	}

	w.Header().Set(types.ContentTypeHeader, contentType)
	if obj.Size >= 0 {
		w.Header().Set(types.ContentLengthHeader, fmt.Sprint(obj.Size))
	}
	w.Header().Set(types.ContentModifiedHeader, obj.ModTime.Format(http.TimeFormat))

	if metaJSON, err := json.Marshal(obj); err == nil {
		w.Header().Set(schema.ObjectMetaHeader, string(metaJSON))
	}

	w.WriteHeader(http.StatusOK)
	return nil
}

func objectGet(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	pathValue := r.PathValue("path")
	reader, obj, err := mgr.ReadObject(r.Context(), schema.ReadObjectRequest{
		Name: r.PathValue("name"),
		Path: "/" + pathValue,
	})
	if err != nil {
		return httpresponse.Error(w, err)
	}
	defer reader.Close()

	buffer := make([]byte, 512)
	n, err := io.ReadFull(reader, buffer)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return httpresponse.Error(w, err)
	}

	contentType := http.DetectContentType(buffer[:n])
	if contentType == types.ContentTypeBinary {
		if extType := mime.TypeByExtension(filepath.Ext(pathValue)); extType != "" {
			contentType = extType
		}
	}

	w.Header().Set(types.ContentTypeHeader, contentType)
	if obj.Size >= 0 {
		w.Header().Set(types.ContentLengthHeader, fmt.Sprint(obj.Size))
	}
	w.Header().Set(types.ContentModifiedHeader, obj.ModTime.Format(http.TimeFormat))

	if metaJSON, err := json.Marshal(obj); err == nil {
		w.Header().Set(schema.ObjectMetaHeader, string(metaJSON))
	}

	w.WriteHeader(http.StatusOK)

	if n > 0 {
		if _, err := w.Write(buffer[:n]); err != nil {
			return err
		}
	}

	if _, err := io.Copy(w, reader); err != nil {
		return err
	}

	return nil
}
