package httphandler

import (
	"encoding/json"
	"errors"
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
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterObjectHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Objects", "Object Operations")

	return errors.Join(
		router.RegisterPath("object", nil, httprequest.NewPathItem("Objects", "List objects or create a new object").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListObjects(w, r, manager)
				},
				"List objects",
				openapi.WithTags("Objects"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.ObjectListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.ObjectList]()),
			),
		),
		router.RegisterPath("object/{volume}/{path...}", nil, httprequest.NewPathItem("Objects", "Get, update or delete an object").
			Head(
				func(w http.ResponseWriter, r *http.Request) {
					_ = HeadObject(w, r, manager, r.PathValue("volume"), r.PathValue("path"))
				},
				"Get object metadata",
				openapi.WithTags("Objects"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Object]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListObjects(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.ObjectListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if resp, err := manager.ListObjects(r.Context(), req); err != nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}
}

func HeadObject(w http.ResponseWriter, r *http.Request, manager *manager.Manager, volume, path string) error {
	req := schema.ObjectKey{
		Volume: volume,
		Path:   path,
	}

	// Get the object metadata
	obj, err := manager.GetObject(r.Context(), req)
	if err != nil && obj == nil {
		return httpresponse.Error(w, gofiler.HTTPErr(err), types.Stringify(req))
	}

	// TODO: An error occurred, but we have an object, so we continue...

	// Set the content type and disposition headers
	w.Header().Set(types.ContentTypeHeader, obj.ContentType)
	if filename := filepath.Base(obj.Path); filename != "" {
		if cd := mime.FormatMediaType("inline", map[string]string{"filename": filename}); cd != "" {
			w.Header().Set(types.ContentDispositonHeader, cd)
		}
	}
	w.Header().Set(types.ContentPathHeader, obj.Path)

	// Set etag, size and modified
	if etag := types.Value(obj.ETag); etag != "" {
		w.Header().Set(types.ContentHashHeader, etag)
	}
	if obj.Size >= 0 {
		w.Header().Set(types.ContentLengthHeader, strconv.FormatInt(obj.Size, 10))
	}
	w.Header().Set(types.ContentModifiedHeader, obj.ModTime.Format(http.TimeFormat))

	// Set the object header
	if data, err := json.Marshal(obj); err == nil {
		w.Header().Set(schema.ContentObjectHeader, string(data))
	}

	// Determine the response code based on the preconditions
	return checkPreconditions(w, r, obj)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func checkPreconditions(w http.ResponseWriter, r *http.Request, obj *schema.Object) error {
	etag := types.Value(obj.ETag)
	modtime := obj.ModTime

	if im := r.Header.Get(schema.ContentIfMatchHeader); im != "" {
		if !matchETags(im, etag, true) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return nil
		}
	} else if ius := r.Header.Get(schema.ContentIfUnmodifiedSinceHeader); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && modtime.After(t) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return nil
		}
	}

	if inm := r.Header.Get(schema.ContentIfNoneMatchHeader); inm != "" {
		if matchETags(inm, etag, false) {
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
	} else if ims := r.Header.Get(schema.ContentIfModifiedSinceHeader); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !modtime.After(t) {
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
	}

	w.WriteHeader(http.StatusOK)
	return nil
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
