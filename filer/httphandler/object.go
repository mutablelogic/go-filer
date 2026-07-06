package httphandler

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

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

type ObjectPath struct {
	Volume string `json:"volume" help:"Volume name"`
	Path   string `json:"path" help:"Object path"`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// RegisterObjectHandlers registers the object handlers
func RegisterObjectHandlers(manager *manager.Filer, router *httprouter.Router) error {
	router.Spec().AddTag("Objects", documentation.Section(2, "Object Operations").Body)
	return errors.Join(
		router.Register("object", nil, func(path httprequest.PathItem) {
			path.Tag("Objects")

			// GET
			path.Get(func(w http.ResponseWriter, r *http.Request) {
				var req schema.ObjectListRequest
				if err := httprequest.Query(r.URL.Query(), &req); err != nil {
					httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
				} else if resp, err := manager.ListObjects(r.Context(), req); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("List Objects")
				op.Description(documentation.Section(3, "GET /object").Body)
				op.Query(jsonschema.MustFor[schema.ObjectListRequest]())
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.ObjectList](), "List of objects")
				op.ErrorResponse(http.StatusBadRequest, "Invalid request")
				op.ErrorResponse(http.StatusNotFound, "Volume not found")
				op.ErrorResponse(http.StatusServiceUnavailable, "Volume not mounted")
			})
		}),
		router.Register("object/{volume}/{path...}", jsonschema.MustFor[ObjectPath](), func(path httprequest.PathItem) {
			path.Tag("Objects")

			// HEAD
			path.Head(func(w http.ResponseWriter, r *http.Request) {
				req := schema.ObjectKey{
					Volume: r.PathValue("volume"),
					Path:   r.PathValue("path"),
				}

				// Get the object metadata
				obj, err := manager.GetObject(r.Context(), req)
				if err != nil && obj == nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
					return
				}

				// TODO: An error occurred, but we have an object, so we continue...

				setObjectHeaders(w, obj)

				// Determine the response code based on the preconditions
				checkPreconditions(w, r, obj)
			}, func(op httprequest.PathOperation) {
				op.Summary("Get Object Metadata")
				op.Description(documentation.Section(3, "HEAD /object/{volume}/{path...}").Body)
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Object](), "Object metadata")
				op.Response(http.StatusNotModified, "", "Object matches the conditional request headers")
				op.ErrorResponse(http.StatusBadRequest, "Invalid volume or object path")
				op.ErrorResponse(http.StatusNotFound, "Volume or object not found")
				op.ErrorResponse(http.StatusPreconditionFailed, "Object does not match the conditional request headers")
				op.ErrorResponse(http.StatusServiceUnavailable, "Volume not mounted")
			})

			// GET
			path.Get(func(w http.ResponseWriter, r *http.Request) {
				req := schema.ObjectKey{
					Volume: r.PathValue("volume"),
					Path:   r.PathValue("path"),
				}

				reader, obj, err := manager.ReadObject(r.Context(), req, readObjectAttrFromRequest(r))
				if errors.Is(err, gofiler.ErrNotModified) {
					if obj != nil {
						setObjectHeaders(w, obj)
					}
					w.WriteHeader(http.StatusNotModified)
					return
				}
				if err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
					return
				}
				defer reader.Close()

				setObjectHeaders(w, obj)
				w.Header().Set("Cache-Control", "private, max-age=0, must-revalidate")
				if status := preconditionStatus(r, obj); status != http.StatusOK {
					w.WriteHeader(status)
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = io.Copy(w, reader)
			}, func(op httprequest.PathOperation) {
				op.Summary("Get Object")
				op.Description(documentation.Section(3, "GET /object/{volume}/{path...}").Body)
				op.Response(http.StatusOK, types.ContentTypeBinary, "Object content")
				op.Response(http.StatusNotModified, "", "Object matches the conditional request headers")
				op.ErrorResponse(http.StatusBadRequest, "Invalid volume or object path")
				op.ErrorResponse(http.StatusNotFound, "Volume or object not found")
				op.ErrorResponse(http.StatusPreconditionFailed, "Object does not match the conditional request headers")
				op.ErrorResponse(http.StatusServiceUnavailable, "Volume not mounted")
			})

			// PUT
			path.Put(func(w http.ResponseWriter, r *http.Request) {
				req := schema.CreateObjectRequest{
					ObjectKey: schema.ObjectKey{
						Volume: r.PathValue("volume"),
						Path:   r.PathValue("path"),
					},
					Body: r.Body,
				}
				attr := schema.ObjectAttr{}

				if contentType := r.Header.Get(types.ContentTypeHeader); contentType != "" {
					req.ContentType = contentType
				}
				if etag := r.Header.Get(types.ContentHashHeader); etag != "" {
					attr.ETag = types.Ptr(etag)
				}
				if r.ContentLength >= 0 {
					attr.Size = types.Ptr(r.ContentLength)
				} else if size := r.Header.Get(types.ContentLengthHeader); size != "" {
					if value, err := strconv.ParseInt(size, 10, 64); err != nil {
						httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()), types.Stringify(req))
						return
					} else {
						attr.Size = types.Ptr(value)
					}
				}
				if modified := r.Header.Get(types.ContentModifiedHeader); modified != "" {
					if value, err := http.ParseTime(modified); err != nil {
						httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()), types.Stringify(req))
						return
					} else {
						req.ModTime = value
						attr.ModTime = value
					}
				}
				if strings.TrimSpace(r.Header.Get(schema.ContentIfNoneMatchHeader)) == "*" {
					req.IfNotExists = true
				}

				obj, created, err := manager.CreateObject(r.Context(), req, attr)
				if err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
				} else if created {
					httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), obj)
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), obj)
				}
			}, func(op httprequest.PathOperation) {
				op.Summary("Create or Update Object")
				op.Description(documentation.Section(3, "PUT /object/{volume}/{path...}").Body)
				op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Object](), "Updated object")
				op.JSONResponse(http.StatusCreated, jsonschema.MustFor[schema.Object](), "Created object")
				op.Response(http.StatusNotModified, "", "Object already matches the request headers")
				op.ErrorResponse(http.StatusBadRequest, "Invalid volume or object path")
				op.ErrorResponse(http.StatusNotFound, "Volume or object not found")
				op.ErrorResponse(http.StatusConflict, "Object already exists")
				op.ErrorResponse(http.StatusPreconditionFailed, "Object does not match the conditional request headers")
				op.ErrorResponse(http.StatusServiceUnavailable, "Volume not mounted")
			})
		}),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func checkPreconditions(w http.ResponseWriter, r *http.Request, obj *schema.Object) {
	w.WriteHeader(preconditionStatus(r, obj))
}

func preconditionStatus(r *http.Request, obj *schema.Object) int {
	etag := types.Value(obj.ETag)
	modtime := obj.ModTime

	if im := r.Header.Get(schema.ContentIfMatchHeader); im != "" {
		if !matchETags(im, etag, true) {
			return http.StatusPreconditionFailed
		}
	} else if ius := r.Header.Get(schema.ContentIfUnmodifiedSinceHeader); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && modtime.After(t) {
			return http.StatusPreconditionFailed
		}
	}

	if inm := r.Header.Get(schema.ContentIfNoneMatchHeader); inm != "" {
		if matchETags(inm, etag, false) {
			return http.StatusNotModified
		}
	} else if ims := r.Header.Get(schema.ContentIfModifiedSinceHeader); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !modtime.After(t) {
			return http.StatusNotModified
		}
	}

	return http.StatusOK
}

func setObjectHeaders(w http.ResponseWriter, obj *schema.Object) {
	w.Header().Set(types.ContentTypeHeader, obj.ContentType)
	if filename := filepath.Base(obj.Path); filename != "" {
		if cd := mime.FormatMediaType("inline", map[string]string{"filename": filename}); cd != "" {
			w.Header().Set(types.ContentDispositonHeader, cd)
		}
	}
	w.Header().Set(types.ContentPathHeader, obj.Path)
	if etag := types.Value(obj.ETag); etag != "" {
		w.Header().Set(types.ContentHashHeader, etag)
	}
	if obj.Size != nil && types.Value(obj.Size) >= 0 {
		w.Header().Set(types.ContentLengthHeader, strconv.FormatInt(types.Value(obj.Size), 10))
	}
	if !obj.ModTime.IsZero() {
		w.Header().Set(types.ContentModifiedHeader, obj.ModTime.Format(http.TimeFormat))
	}
	if data, err := json.Marshal(obj); err == nil {
		w.Header().Set(schema.ContentObjectHeader, string(data))
	}
}

func readObjectAttrFromRequest(r *http.Request) schema.ObjectAttr {
	var attr schema.ObjectAttr
	if inm := strings.TrimSpace(r.Header.Get(schema.ContentIfNoneMatchHeader)); inm != "" && inm != "*" && !strings.Contains(inm, ",") {
		etag := strings.Trim(strings.TrimPrefix(inm, "W/"), `"`)
		if etag != "" {
			attr.ETag = types.Ptr(etag)
		}
	}
	if ims := r.Header.Get(schema.ContentIfModifiedSinceHeader); ims != "" {
		if modified, err := http.ParseTime(ims); err == nil {
			attr.ModTime = modified
		}
	}
	return attr
}

func matchETags(header, etag string, strong bool) bool {
	if strings.TrimSpace(header) == "*" {
		return etag != ""
	}
	if strong && strings.HasPrefix(etag, "W/") {
		return false
	}
	for part := range strings.SplitSeq(header, ",") {
		part = strings.TrimSpace(part)
		if strong && strings.HasPrefix(part, "W/") {
			continue
		}
		if strings.Trim(strings.TrimPrefix(part, "W/"), `"`) ==
			strings.Trim(strings.TrimPrefix(etag, "W/"), `"`) {
			return true
		}
	}
	return false
}
