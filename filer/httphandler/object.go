package httphandler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// objectName is the path of the objects in a volume
type objectName struct {
	Name string `json:"name" help:"Volume name"`
}

// objectKey is the path of an object in a volume
type objectKey struct {
	Name string `json:"name" help:"Volume name"`
	Path string `json:"path" help:"Path of the object in the volume, which is the root when it's empty"`
}

// objectGetQuery is the query for getting the content of a file
type objectGetQuery struct {
	Download bool `json:"download,omitempty" help:"Download the file, as an attachment, rather than showing it in the browser"`
}

// objectUploadQuery is the query for uploading files into a directory
type objectUploadQuery struct {
	Overwrite bool `json:"overwrite,omitempty" help:"Replace files which exist, rather than skipping them"`
}

// objectUploadForm is the form of files uploaded into a directory
type objectUploadForm struct {
	File []string `json:"file" format:"binary" help:"Files, whose names are their paths in the directory, which can have directories"`
}

// objectDeleteQuery is the query for deleting an object
type objectDeleteQuery struct {
	Recursive bool `json:"recursive,omitempty" help:"Delete a directory, and every object beneath it"`
}

// objectListQuery is the query for listing objects
type objectListQuery struct {
	Path       string  `json:"path,omitempty" help:"Directory to list, which defaults to the root of the volume"`
	Recursive  bool    `json:"recursive,omitempty" help:"List every object beneath the directory"`
	StartAfter string  `json:"start_after,omitempty" help:"Return objects whose keys sort after this key"`
	Limit      *uint64 `json:"limit,omitempty" help:"Maximum number of objects to return, which defaults to and is capped at 100"`
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func objectHandlers(registry *filer.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	router.Spec().AddTag("Objects", documentation.Section(2, "Object Operations").Body)
	return errors.Join(
		objectListHandlers(registry, router, documentation),
		objectHandler(registry, router, documentation),
	)
}

func objectHandler(registry *filer.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("object/{name}/{path...}", jsonschema.MustFor[objectKey](), func(path httprequest.PathItem) {
		path.Tag("Objects")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			objectGet(registry, w, r)
		}, func(op httprequest.PathOperation) {
			op.Summary("Get Object")
			op.Description(documentation.Section(3, "GET /object/{name}/{path...}").Body)
			op.Query(jsonschema.MustFor[objectGetQuery]())
			op.Response(http.StatusOK, "*/*", "File's content, of its type, with its metadata as JSON in the X-Meta header")
			op.Response(http.StatusPartialContent, "*/*", "Range of the file's content, when the file can seek, such as a file on disk")
			op.Response(http.StatusRequestedRangeNotSatisfiable, types.ContentTypeTextPlain, "Range not satisfiable")
			op.Response(http.StatusTemporaryRedirect, "", "A directory, which redirects to its listing")
			op.Response(http.StatusNotModified, "", "File not modified since If-Modified-Since, or matching If-None-Match")
			op.ErrorResponse(http.StatusPreconditionFailed, "File doesn't match If-Match, or was modified since If-Unmodified-Since, or changed while it was read")
			op.ErrorResponse(http.StatusBadRequest, "Invalid path or query")
			op.ErrorResponse(http.StatusNotFound, "Volume isn't mounted, or object not found")
			op.ErrorResponse(http.StatusNotImplemented, "The object is a package, which can't be read")
		})

		// PUT
		path.Put(func(w http.ResponseWriter, r *http.Request) {
			objectPut(registry, w, r)
		}, func(op httprequest.PathOperation) {
			op.Summary("Upload Object")
			op.Description(documentation.Section(3, "PUT /object/{name}/{path...}").Body)
			op.JSONResponse(http.StatusCreated, jsonschema.MustFor[schema.Object](), "File which was created")
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Object](), "File which was replaced")
			op.ErrorResponse(http.StatusBadRequest, "Invalid path, or the root of the volume, or If-None-Match or If-Match isn't *, or the body ends before it should")
			op.ErrorResponse(http.StatusForbidden, "Volume is read-only")
			op.ErrorResponse(http.StatusNotFound, "Volume isn't mounted")
			op.ErrorResponse(http.StatusConflict, "A directory is at the path")
			op.ErrorResponse(http.StatusPreconditionFailed, "The file exists, with If-None-Match: *, or doesn't, with If-Match: *")
			op.ErrorResponse(http.StatusUnprocessableEntity, "The file can't be stored, as its name is skipped, such as a hidden file, or it's in a package")
			op.ErrorResponse(http.StatusNotImplemented, "Content-Type isn't the type of the file's extension")
		})

		// POST
		path.Post(func(w http.ResponseWriter, r *http.Request) {
			objectUpload(registry, w, r)
		}, func(op httprequest.PathOperation) {
			op.Summary("Upload Objects")
			op.Description(documentation.Section(3, "POST /object/{name}/{path...}").Body)
			op.Query(jsonschema.MustFor[objectUploadQuery]())
			op.RequestBody(jsonschema.MustFor[objectUploadForm](), types.ContentTypeFormData)
			op.JSONResponse(http.StatusCreated, jsonschema.MustFor[schema.ObjectUploadResponse](), "Files which were written, and those which were skipped")
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.ObjectUploadResponse](), "No files were written, as every file was skipped")
			op.ErrorResponse(http.StatusBadRequest, "Invalid path or query, or the body isn't a multipart form, or ends before it should")
			op.ErrorResponse(http.StatusForbidden, "Volume is read-only")
			op.ErrorResponse(http.StatusNotFound, "Volume isn't mounted")
			op.ErrorResponse(http.StatusConflict, "The path is a file, not a directory")
		})

		// DELETE
		path.Delete(func(w http.ResponseWriter, r *http.Request) {
			var query objectDeleteQuery
			if key, err := objectKeyFromRequest(r); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("name")+"/"+r.PathValue("path"))
			} else if err := httprequest.Query(r.URL.Query(), &query); err != nil {
				httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("%w", err))
			} else if deleted, err := registry.DeleteObjects(r.Context(), schema.DeleteObjectsRequest{ObjectKey: key, Recursive: query.Recursive}); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), key.Name+key.Path)
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), deleted)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Delete Object")
			op.Description(documentation.Section(3, "DELETE /object/{name}/{path...}").Body)
			op.Query(jsonschema.MustFor[objectDeleteQuery]())
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.DeleteObjectsResponse](), "Deleted objects, which are files, not directories")
			op.ErrorResponse(http.StatusBadRequest, "Invalid path or query, or the root of the volume, or a path which is hidden")
			op.ErrorResponse(http.StatusForbidden, "Volume is read-only")
			op.ErrorResponse(http.StatusNotFound, "Volume isn't mounted, or object not found")
			op.ErrorResponse(http.StatusConflict, "The object is a directory, and the request isn't recursive")
		})

		// HEAD
		path.Head(func(w http.ResponseWriter, r *http.Request) {
			key, err := objectKeyFromRequest(r)
			if err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("name")+"/"+r.PathValue("path"))
			} else if object, err := registry.GetObject(r.Context(), schema.GetObjectRequest{ObjectKey: key}); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), key.Name+key.Path)
			} else if err := objectBinary(object).Write(w, r, nil); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), key.Name+key.Path)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Get Object Metadata")
			op.Description(documentation.Section(3, "HEAD /object/{name}/{path...}").Body)
			op.Response(http.StatusOK, "*/*", "Object or directory, with its metadata as JSON in the X-Meta header, and no body. A directory's type is inode/directory.")
			op.Response(http.StatusNotModified, "", "Object not modified since If-Modified-Since, or matching If-None-Match")
			op.ErrorResponse(http.StatusPreconditionFailed, "Object doesn't match If-Match, or was modified since If-Unmodified-Since")
			op.ErrorResponse(http.StatusBadRequest, "Invalid path")
			op.ErrorResponse(http.StatusNotFound, "Volume isn't mounted, or object not found")
		})
	})
}

func objectListHandlers(registry *filer.Registry, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("object/{name}", jsonschema.MustFor[objectName](), func(path httprequest.PathItem) {
		path.Tag("Objects")

		// GET, as JSON, or as HTML when it's accepted before JSON, so the
		// response varies with Accept
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Vary", "Accept")
			var query objectListQuery
			if err := httprequest.Query(r.URL.Query(), &query); err != nil {
				httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("%w", err))
			} else if key, err := cleanPath(query.Path); err != nil {
				httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("name")+"/"+query.Path)
			} else {
				req := schema.ObjectListRequest{Limit: query.Limit}
				req.Name, req.Path = r.PathValue("name"), key
				req.Recursive, req.StartAfter = query.Recursive, query.StartAfter
				if list, err := registry.List(r.Context(), req); err != nil {
					httpresponse.Error(w, gofiler.HTTPErr(err), req.Name+req.Path)
				} else if acceptsHTML(r) {
					if err := writeListHTML(w, r, list, readonly(registry, req.Name)); err != nil {
						httpresponse.Error(w, httpresponse.ErrInternalError.Withf("%v", err), req.Name+req.Path)
					}
				} else {
					httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), list)
				}
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("List Objects")
			op.Description(documentation.Section(3, "GET /object/{name}").Body)
			op.Query(jsonschema.MustFor[objectListQuery]())
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.ObjectList](), "Objects")
			op.Response(http.StatusOK, "text/html", "Objects, as a page of HTML, when text/html is accepted before JSON")
			op.ErrorResponse(http.StatusBadRequest, "Invalid path or query")
			op.ErrorResponse(http.StatusNotFound, "Volume isn't mounted, or path not found")
		})
	})
}

// objectGet responds with the content of a file, with the headers a HEAD
// request has, or redirects to the listing of a directory. The content is
// shown in the browser, or downloaded as an attachment, with the file's
// name, and isn't sniffed as another type, and HTML, SVG and other XML is
// sandboxed. A file which isn't modified isn't read. When the volume is
// removed while the content is written, which is cancelled, a write which
// is blocked, as the client is slow to read it, returns, so the volume isn't
// held up.
func objectGet(registry *filer.Registry, w http.ResponseWriter, r *http.Request) {
	var query objectGetQuery
	key, err := objectKeyFromRequest(r)
	if err != nil {
		httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("name")+"/"+r.PathValue("path"))
		return
	} else if err := httprequest.Query(r.URL.Query(), &query); err != nil {
		httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("%w", err))
		return
	}

	// A directory redirects to its listing, which is the path of the request
	// without the object's path
	object, err := registry.GetObject(r.Context(), schema.GetObjectRequest{ObjectKey: key})
	if err != nil {
		httpresponse.Error(w, gofiler.HTTPErr(err), key.Name+key.Path)
		return
	} else if object.IsDir {
		listing := strings.TrimSuffix(strings.TrimSuffix(r.URL.Path, r.PathValue("path")), "/")
		if key.Path != "/" {
			listing += "?" + url.Values{"path": {key.Path}}.Encode()
		}
		http.Redirect(w, r, listing, http.StatusTemporaryRedirect)
		return
	}

	// The headers alone, when it isn't modified, or a precondition fails,
	// without reading it
	binary := objectBinary(object)
	binary.Disposition = disposition(query.Download)
	if binary.NotModified(r) || !preconditions(r, binary) {
		if err := binary.Write(w, r, nil); err != nil {
			httpresponse.Error(w, gofiler.HTTPErr(err), key.Name+key.Path)
		}
		return
	}

	// Read the file, whose metadata is the object's, and write it, after
	// which an error can't be responded with
	read, object, err := registry.ReadObject(r.Context(), schema.GetObjectRequest{ObjectKey: key})
	if err != nil {
		httpresponse.Error(w, gofiler.HTTPErr(err), key.Name+key.Path)
		return
	}
	binary = objectBinary(object)
	binary.Disposition = disposition(query.Download)
	var written bool
	rc := http.NewResponseController(w)
	err = read(r.Context(), func(body io.Reader) error {
		stop := context.AfterFunc(r.Context(), func() { rc.SetWriteDeadline(time.Now()) })
		defer stop()
		written = true
		return binary.Write(w, r, body)
	})
	if err != nil && !written {
		httpresponse.Error(w, gofiler.HTTPErr(err), key.Name+key.Path)
	}
}

// readonly returns true when the volume is read-only, by its options, so its
// objects can't be deleted, or it isn't mounted
func readonly(registry *filer.Registry, name string) bool {
	volume, err := registry.Volume(name)
	if err != nil {
		return true
	}
	u, err := url.Parse(volume.URL)
	if err != nil {
		return true
	}
	value, set := u.Query()["readonly"]
	return set && (len(value) == 0 || value[0] == "" || value[0] == "true" || value[0] == "1")
}

// objectPut writes the request's body to the file at the path, which is
// created, with 201, or replaced, with 200, and the directories it's in. With
// If-None-Match: *, a file which exists isn't replaced, and with If-Match: *,
// a file which doesn't exist isn't created, which are 412. The file's
// modified time is the request's Last-Modified, when it's set. Its type is
// from its extension, so a Content-Type which isn't, other than
// application/octet-stream, isn't supported. The body is read without the
// server's read timeout, as a large file can take longer.
func objectPut(registry *filer.Registry, w http.ResponseWriter, r *http.Request) {
	key, err := objectKeyFromRequest(r)
	if err != nil {
		httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("name")+"/"+r.PathValue("path"))
		return
	} else if key.Path == "/" {
		httpresponse.Error(w, httpresponse.ErrBadRequest.With("the root of a volume is a directory, which can't be uploaded to"), key.Name+key.Path)
		return
	}
	ifNotExists, ifExists := r.Header.Get("If-None-Match") == "*", r.Header.Get("If-Match") == "*"
	if (r.Header.Get("If-None-Match") != "" && !ifNotExists) || (r.Header.Get("If-Match") != "" && !ifExists) || (ifNotExists && ifExists) {
		httpresponse.Error(w, httpresponse.ErrBadRequest.With("If-None-Match and If-Match can only be *, and not both"), key.Name+key.Path)
		return
	}
	req := schema.CreateObjectRequest{ObjectKey: key, Body: r.Body, IfNotExists: ifNotExists, IfExists: ifExists}
	if modified := r.Header.Get(types.ContentModifiedHeader); modified != "" {
		if req.ModTime, err = http.ParseTime(modified); err != nil {
			httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("invalid %s: %q", types.ContentModifiedHeader, modified), key.Name+key.Path)
			return
		}
	}
	if contentType := r.Header.Get(types.ContentTypeHeader); contentType != "" {
		if mediaType, _, err := mime.ParseMediaType(contentType); err != nil || mediaType != types.ContentTypeBinary {
			req.ContentType = contentType
		}
	}

	// Whether the file exists, so it's 201 when it's created, and a file
	// which exists, with If-None-Match: *, or doesn't, with If-Match: *, is
	// 412 before the body is read. A directory at the path is a conflict.
	existing, err := registry.GetObject(r.Context(), schema.GetObjectRequest{ObjectKey: key})
	exists := err == nil
	switch {
	case err != nil && !errors.Is(err, fs.ErrNotExist):
		// The volume isn't mounted, or the path is invalid
		httpresponse.Error(w, gofiler.HTTPErr(err), key.Name+key.Path)
		return
	case exists && existing.IsDir:
		httpresponse.Error(w, httpresponse.ErrConflict.Withf("%q is a directory", key.Path), key.Name+key.Path)
		return
	case exists && ifNotExists, !exists && ifExists:
		httpresponse.Error(w, httpresponse.Err(http.StatusPreconditionFailed), key.Name+key.Path)
		return
	}

	// Write the file, without the server's read timeout. A file which is
	// created, or removed, in the meantime, is 412 too.
	if err := http.NewResponseController(w).SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		httpresponse.Error(w, httpresponse.ErrInternalError.Withf("%v", err), key.Name+key.Path)
		return
	}
	object, err := registry.CreateObject(r.Context(), req)
	switch {
	case ifNotExists && errors.Is(err, gofiler.ErrConflict), errors.Is(err, gofiler.ErrPreconditionFailed):
		httpresponse.Error(w, httpresponse.Err(http.StatusPreconditionFailed), key.Name+key.Path)
	case err != nil:
		httpresponse.Error(w, gofiler.HTTPErr(err), key.Name+key.Path)
	case exists:
		httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), object)
	default:
		httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), object)
	}
}

// objectUpload writes each file of the request's multipart form, whose form
// name is "file", into the directory at the path, which is created, with
// the directories they're in. A file's name is its path in the directory,
// which can have directories, but not be outside it. Each file is read from
// the form as it's written, so they aren't buffered. A file which exists is
// skipped, unless the upload overwrites it, as is one which can't be
// stored, such as one whose name is skipped, or one which is a directory,
// and the response has them, and why. It's 201 when a file is written, and
// otherwise 200. An error which isn't of a file stops the upload, when the
// files written are kept.
func objectUpload(registry *filer.Registry, w http.ResponseWriter, r *http.Request) {
	var query objectUploadQuery
	key, err := objectKeyFromRequest(r)
	if err != nil {
		httpresponse.Error(w, gofiler.HTTPErr(err), r.PathValue("name")+"/"+r.PathValue("path"))
		return
	} else if err := httprequest.Query(r.URL.Query(), &query); err != nil {
		httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("%w", err))
		return
	}

	// The directory, which is created, when it doesn't exist, isn't a file
	if dir, err := registry.GetObject(r.Context(), schema.GetObjectRequest{ObjectKey: key}); err != nil && !errors.Is(err, fs.ErrNotExist) {
		httpresponse.Error(w, gofiler.HTTPErr(err), key.Name+key.Path)
		return
	} else if err == nil && !dir.IsDir {
		httpresponse.Error(w, httpresponse.ErrConflict.Withf("%q is a file, not a directory", key.Path), key.Name+key.Path)
		return
	}
	reader, err := r.MultipartReader()
	if err != nil {
		httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("%v", err), key.Name+key.Path)
		return
	}

	// Write each file, without the server's read timeout
	if err := http.NewResponseController(w).SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		httpresponse.Error(w, httpresponse.ErrInternalError.Withf("%v", err), key.Name+key.Path)
		return
	}
	resp := schema.ObjectUploadResponse{Body: []*schema.Object{}}
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("%v", err), key.Name+key.Path)
			return
		} else if part.FormName() != "file" {
			continue
		}

		// The file's path, which is its name in the directory, which isn't
		// outside it
		name := partFileName(part)
		file := path.Join(key.Path, name)
		if name == "" || !strings.HasPrefix(file, strings.TrimSuffix(key.Path, "/")+"/") {
			resp.Skipped = append(resp.Skipped, schema.ObjectSkipped{Path: path.Join(key.Path, name), Reason: "the file's name isn't a path in the directory"})
			continue
		}

		// A file which exists isn't overwritten, unless the upload does, and
		// isn't read, and a directory isn't
		if object, err := registry.GetObject(r.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Name: key.Name, Path: file}}); err == nil {
			if object.IsDir {
				resp.Skipped = append(resp.Skipped, schema.ObjectSkipped{Path: file, Reason: "a directory is at the path"})
				continue
			} else if !query.Overwrite {
				resp.Skipped = append(resp.Skipped, schema.ObjectSkipped{Path: file, Exists: true, Reason: "the file exists"})
				continue
			}
		}

		// Write the file, with the part's Last-Modified, when it's set, which
		// is skipped when it can't be stored, or it's created in the meantime
		req := schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Name: key.Name, Path: file}, Body: part, IfNotExists: !query.Overwrite}
		if modified := part.Header.Get(types.ContentModifiedHeader); modified != "" {
			if req.ModTime, err = http.ParseTime(modified); err != nil {
				resp.Skipped = append(resp.Skipped, schema.ObjectSkipped{Path: file, Reason: fmt.Sprintf("invalid %s: %q", types.ContentModifiedHeader, modified)})
				continue
			}
		}
		object, err := registry.CreateObject(r.Context(), req)
		switch {
		case err == nil:
			resp.Body = append(resp.Body, object)
		case !query.Overwrite && errors.Is(err, gofiler.ErrConflict):
			resp.Skipped = append(resp.Skipped, schema.ObjectSkipped{Path: file, Exists: true, Reason: "the file exists"})
		case errors.Is(err, gofiler.ErrUnprocessableContent), errors.Is(err, gofiler.ErrConflict), errors.Is(err, fs.ErrInvalid):
			resp.Skipped = append(resp.Skipped, schema.ObjectSkipped{Path: file, Reason: err.Error()})
		default:
			httpresponse.Error(w, gofiler.HTTPErr(err), key.Name+file)
			return
		}
	}

	// Return the files, which is 201 when a file was written
	if len(resp.Body) > 0 {
		httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), resp)
	} else {
		httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}
}

// partFileName returns the file name of a part of a multipart form, which
// is its path, as a browser sends a directory's files, with "/", which
// Part.FileName removes. A name with "\" is a path too, as Windows has.
func partFileName(part *multipart.Part) string {
	_, params, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
	if err != nil {
		return ""
	}
	return strings.ReplaceAll(params["filename"], "\\", "/")
}

// disposition returns how the browser presents a file: as an attachment,
// which it saves, when it's downloaded, or inline
func disposition(download bool) httpresponse.Disposition {
	if download {
		return httpresponse.DispositionAttachment
	}
	return httpresponse.DispositionInline
}

// preconditions returns false when If-Match, or without it,
// If-Unmodified-Since, fails, as a response to which has no content
func preconditions(r *http.Request, binary httpresponse.Binary) bool {
	if match := r.Header.Get("If-Match"); match != "" {
		return match == "*"
	} else if since, err := http.ParseTime(r.Header.Get("If-Unmodified-Since")); err == nil && !binary.ModTime.IsZero() {
		return !binary.ModTime.Truncate(time.Second).After(since)
	}
	return true
}

// objectKeyFromRequest returns the volume and path of an object from the
// path of the request, whose elements are separated with /, not %2F, which
// would be a name with a / in it
func objectKeyFromRequest(r *http.Request) (schema.ObjectKey, error) {
	if strings.Contains(strings.ToUpper(r.URL.EscapedPath()), "%2F") {
		return schema.ObjectKey{}, gofiler.ErrBadParameter.With("the elements of a path are separated with /, not %2F")
	}
	path, err := cleanPath(r.PathValue("path"))
	if err != nil {
		return schema.ObjectKey{}, err
	}
	return schema.ObjectKey{Name: r.PathValue("name"), Path: path}, nil
}

// objectBinary returns the headers of an object's response, with its
// metadata as JSON in the X-Meta header. A directory has no size, and its
// type is inode/directory.
func objectBinary(object *schema.Object) httpresponse.Binary {
	binary := httpresponse.Binary{
		ContentType: object.ContentType,
		Size:        object.Size,
		ModTime:     object.ModTime,
		ETag:        types.Value(object.ETag),
		Path:        object.Path,
		Meta:        object,
	}
	if object.IsDir {
		binary.ContentType, binary.Size = schema.DirectoryType, nil
	}
	return binary
}

// cleanPath returns the path of an object, which starts with "/", resolving
// "." and ".." elements, or ErrBadParameter when it's outside the volume
func cleanPath(key string) (string, error) {
	clean := path.Clean(strings.TrimPrefix(key, "/"))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", gofiler.ErrBadParameter.Withf("path is outside the volume: %q", key)
	} else if clean == "." {
		return "/", nil
	}
	return "/" + clean, nil
}
