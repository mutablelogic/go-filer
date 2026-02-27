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
	manager "github.com/mutablelogic/go-filer/pkg/manager"
	schema "github.com/mutablelogic/go-filer/pkg/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	openapi "github.com/mutablelogic/go-server/pkg/openapi/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// HANDLER FUNCTIONS

// Path: /{name}/{path...}
// GET downloads a file, HEAD returns metadata, PUT creates/replaces, POST uploads via
// multipart/form-data (field name: "file", repeatable for multiple files), DELETE removes.
func ObjectHandler(mgr *manager.Manager) (string, http.HandlerFunc, *openapi.PathItem) {
	return "/{name}/{path...}", func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				_ = objectGet(w, r, mgr)
			case http.MethodHead:
				_ = objectHead(w, r, mgr)
			case http.MethodPut:
				_ = objectPut(w, r, mgr)
			case http.MethodPost:
				_ = objectUpload(w, r, mgr)
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
			Post: &openapi.Operation{
				Description: "Upload one or more files using multipart/form-data (field name: \"file\", repeatable)",
			},
			Delete: &openapi.Operation{
				Description: "Delete a file or series of files",
			},
		})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func objectPut(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	req := schema.CreateObjectRequest{
		Path: types.NormalisePath(r.PathValue("path")),
		Body: r.Body,
	}

	// Forward Content-Type if provided
	if ct := r.Header.Get(types.ContentTypeHeader); ct != "" {
		req.ContentType = ct
	}

	// Forward Last-Modified if provided
	if lm := r.Header.Get(types.ContentModifiedHeader); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			req.ModTime = t
		}
	}

	// Forward X-Meta-{key} headers as user-defined metadata (lowercased for S3 compatibility)
	req.Meta = extractMeta(r.Header)

	// Create the object in the manager
	obj, err := mgr.CreateObject(r.Context(), r.PathValue("name"), req)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), obj)
}

func objectDelete(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	var request schema.DeleteObjectsRequest

	// Read query parameters into request struct
	if err := httprequest.Query(r.URL.Query(), &request); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else {
		request.Path = types.NormalisePath(r.PathValue("path"))
	}

	// Delete many objects when recursive=true
	if request.Recursive {
		resp, err := mgr.DeleteObjects(r.Context(), r.PathValue("name"), request)
		if err != nil {
			return httpresponse.Error(w, err)
		}
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}

	// Else delete a single object
	obj, err := mgr.DeleteObject(r.Context(), r.PathValue("name"), schema.DeleteObjectRequest{
		Path: request.Path,
	})
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), obj)
}

func objectUpload(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	// Read multipart form data into a struct with a []types.File field. The form reader
	var form struct {
		Files []types.File `json:"file"`
	}
	if err := httprequest.Read(r, &form); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	} else if len(form.Files) == 0 {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(`missing or unreadable "file" form field`))
	}

	// Shared metadata from X-Meta-{key} request headers applies to all files.
	sharedMeta := extractMeta(r.Header)

	// Base path is derived from URL, with trailing slash indicating directory upload.
	// NOTE: types.NormalisePath strips trailing slashes, so we check r.URL.Path
	// directly to preserve the caller's intent (e.g. POST /media/subdir/ vs /media/subdir).
	basePath := types.NormalisePath(r.PathValue("path"))
	name := r.PathValue("name")
	isDir := basePath == "/" || strings.HasSuffix(r.URL.Path, "/")

	// A non-directory path is an explicit destination for exactly one file.
	// Uploading multiple files to the same explicit path would silently
	// overwrite with each successive file; reject it early instead.
	if !isDir && len(form.Files) > 1 {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.Withf(
			"cannot upload %d files to explicit path %q; add a trailing slash to upload to a directory",
			len(form.Files), basePath,
		))
	}

	// Branch to SSE streaming path if the client accepts text/event-stream.
	if accept, _ := types.AcceptContentType(r); accept == types.ContentTypeTextStream {
		return objectUploadSSE(w, r, mgr, form.Files, sharedMeta, name, basePath, isDir)
	}

	// Upload each file in order, tracking created objects so we can roll back on error.
	results := make([]schema.Object, 0, len(form.Files))
	rollback := func(uploadErr error) error {
		var errs []error
		for _, obj := range results {
			if _, err := mgr.DeleteObject(r.Context(), name, schema.DeleteObjectRequest{Path: obj.Path}); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(append([]error{uploadErr}, errs...)...)
	}
	for _, f := range form.Files {
		defer f.Body.Close() //nolint:gocritic // deferred close is intentional per-upload

		// f.Path is already sanitised by httprequest.fileHeaderPath:
		// leading slashes stripped, .. segments resolved, traversal rejected.
		filename := f.Path

		destPath := basePath
		if isDir {
			destPath = types.JoinPath(basePath, filename)
		}

		// Derive content type from part header, falling back to extension sniff.
		contentType := resolveContentType(f.ContentType, "", filepath.Ext(filename))

		// Per-file metadata from part headers overrides shared request-level metadata.
		meta := mergeMeta(sharedMeta, extractMeta(http.Header(f.Header)))

		obj, err := mgr.CreateObject(r.Context(), name, schema.CreateObjectRequest{
			Path:        destPath,
			Body:        f.Body,
			ContentType: contentType,
			Meta:        meta,
		})
		if err != nil {
			return httpresponse.Error(w, rollback(err))
		}
		results = append(results, *obj)
	}

	// Return a JSON array.
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), results)
}

func objectHead(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	obj, err := mgr.GetObject(r.Context(), r.PathValue("name"), schema.GetObjectRequest{
		Path: types.NormalisePath(r.PathValue("path")),
	})
	if err != nil {
		return httpresponse.Error(w, err)
	}

	contentType := resolveContentType(obj.ContentType, "", filepath.Ext(r.PathValue("path")))
	writeObjectHeaders(w, obj, contentType)
	if checkPreconditions(w, r, obj) {
		return nil
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func objectGet(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	reader, obj, err := mgr.ReadObject(r.Context(), r.PathValue("name"), schema.ReadObjectRequest{
		GetObjectRequest: schema.GetObjectRequest{Path: types.NormalisePath(r.PathValue("path"))},
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

	sniffed := http.DetectContentType(buffer[:n])
	contentType := resolveContentType(obj.ContentType, sniffed, filepath.Ext(r.PathValue("path")))
	writeObjectHeaders(w, obj, contentType)
	if checkPreconditions(w, r, obj) {
		return nil
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

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS - HELPER FUNCTIONS

// extractMeta builds an ObjectMeta map from X-Meta-{key} headers, lowercasing
// keys for S3 compatibility. Returns nil if no matching headers are found.
func extractMeta(h http.Header) schema.ObjectMeta {
	var meta schema.ObjectMeta
	for key, vals := range h {
		if after, ok := strings.CutPrefix(key, schema.ObjectMetaKeyPrefix); ok && len(vals) > 0 {
			if meta == nil {
				meta = make(schema.ObjectMeta)
			}
			meta[strings.ToLower(after)] = vals[0]
		}
	}
	return meta
}

// resolveContentType returns the best content-type for an object, preferring
// stored metadata over sniffed body content over file-extension over binary fallback.
func resolveContentType(stored, sniffed, ext string) string {
	if stored != "" && stored != types.ContentTypeBinary {
		return stored
	}
	if sniffed != "" && sniffed != types.ContentTypeBinary {
		return sniffed
	}
	if extType := mime.TypeByExtension(ext); extType != "" {
		return extType
	}
	if stored != "" {
		return stored
	}
	return types.ContentTypeBinary
}

// writeObjectHeaders sets Content-Type, Content-Disposition, Content-Length,
// ETag, Last-Modified and X-Object-Meta response headers from the object metadata.
func writeObjectHeaders(w http.ResponseWriter, obj *schema.Object, contentType string) {
	w.Header().Set(types.ContentTypeHeader, contentType)
	if filename := filepath.Base(obj.Path); filename != "" && filename != "." && filename != "/" {
		if cd := mime.FormatMediaType("inline", map[string]string{"filename": filename}); cd != "" {
			w.Header().Set(types.ContentDispositonHeader, cd)
		}
	}
	w.Header().Set(types.ContentPathHeader, obj.Path)
	if obj.ETag != "" {
		w.Header().Set(types.ContentHashHeader, obj.ETag)
	}
	if obj.Size >= 0 {
		w.Header().Set(types.ContentLengthHeader, strconv.FormatInt(obj.Size, 10))
	}
	w.Header().Set(types.ContentModifiedHeader, obj.ModTime.Format(http.TimeFormat))
	if metaJSON, err := json.Marshal(obj); err == nil {
		w.Header().Set(schema.ObjectMetaHeader, string(metaJSON))
	}
}

// checkPreconditions evaluates RFC 7232 conditional request headers in the
// prescribed order. It writes the appropriate status (304 or 412), returns
// true if the caller should stop processing, and false if the request
// should proceed normally.
func checkPreconditions(w http.ResponseWriter, r *http.Request, obj *schema.Object) bool {
	etag := obj.ETag
	modtime := obj.ModTime

	// 1. If-Match → 412 if no ETag match (strong comparison).
	if im := r.Header.Get("If-Match"); im != "" {
		if !matchETags(im, etag, true) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
	} else if ius := r.Header.Get("If-Unmodified-Since"); ius != "" {
		// 2. If-Unmodified-Since (only when If-Match absent) → 412 if modified.
		if t, err := http.ParseTime(ius); err == nil && modtime.After(t) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
	}

	// 3. If-None-Match → 304 for GET/HEAD if ETag matches (weak comparison).
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if matchETags(inm, etag, false) {
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	} else if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		// 4. If-Modified-Since (only when If-None-Match absent) → 304 if not modified.
		if t, err := http.ParseTime(ims); err == nil && !modtime.After(t) {
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	}

	return false
}

// matchETags reports whether the header value ("*" or a comma-separated list
// of quoted ETags) matches etag. strong=true requires strong comparison;
// strong=false allows weak comparison (W/ prefix stripped before comparing).
func matchETags(header, etag string, strong bool) bool {
	if strings.TrimSpace(header) == "*" {
		return etag != ""
	}
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if strong && strings.HasPrefix(part, "W/") {
			continue // weak ETags never satisfy strong comparison
		}
		if strings.Trim(strings.TrimPrefix(part, "W/"), `"`) ==
			strings.Trim(strings.TrimPrefix(etag, "W/"), `"`) {
			return true
		}
	}
	return false
}

// mergeMeta merges per-file metadata on top of shared metadata.
// Per-file values take precedence over shared values for the same key.
func mergeMeta(shared, perFile schema.ObjectMeta) schema.ObjectMeta {
	if len(shared) == 0 {
		return perFile
	}
	if len(perFile) == 0 {
		return shared
	}
	merged := make(schema.ObjectMeta, len(shared)+len(perFile))
	for k, v := range shared {
		merged[k] = v
	}
	for k, v := range perFile {
		merged[k] = v
	}
	return merged
}

// objectUploadSSE handles a multipart upload and streams progress events to the
// client as Server-Sent Events. All pre-flight validation has already been
// performed by objectUpload before this function is called.
//
// Event sequence:
//
//	start    — once, before the loop; payload: schema.UploadStart
//	file     — written==0 on first emission (file started); then every 64 KiB; payload: schema.UploadFile
//	complete — after each file is committed; payload: schema.Object
//	error    — on failure, after rollback; payload: schema.UploadError; stream closed immediately after
//	done     — all files committed successfully; payload: schema.UploadDone
func objectUploadSSE(
	w http.ResponseWriter,
	r *http.Request,
	mgr *manager.Manager,
	files []types.File,
	sharedMeta schema.ObjectMeta,
	name, basePath string,
	isDir bool,
) error {
	// Open the SSE stream — this commits 200 OK; no HTTP errors are possible after this point.
	stream := httpresponse.NewTextStream(w)

	// Sum declared part sizes for the start event (0 when parts omit Content-Length).
	var declaredBytes int64
	for _, f := range files {
		if v := f.Header.Get(types.ContentLengthHeader); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				declaredBytes += n
			}
		}
	}

	stream.Write(schema.UploadStartEvent, schema.UploadStart{Files: len(files), Bytes: declaredBytes})

	results := make([]schema.Object, 0, len(files))
	var totalWritten int64
	rollback := func(uploadErr error) error {
		var errs []error
		for _, obj := range results {
			if _, err := mgr.DeleteObject(r.Context(), name, schema.DeleteObjectRequest{Path: obj.Path}); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(append([]error{uploadErr}, errs...)...)
	}

	for i, f := range files {
		defer f.Body.Close() //nolint:gocritic // deferred close is intentional per-upload

		filename := f.Path
		destPath := basePath
		if isDir {
			destPath = types.JoinPath(basePath, filename)
		}

		// Declared part size, if the part header provides it (usually absent).
		var partTotal int64
		if v := f.Header.Get(types.ContentLengthHeader); v != "" {
			partTotal, _ = strconv.ParseInt(v, 10, 64)
		}

		// Emit file-start event (written == 0 signals the file has begun).
		stream.Write(schema.UploadFileEvent, schema.UploadFile{
			Index:   i,
			Path:    destPath,
			Written: 0,
			Bytes:   partTotal,
		})

		// Capture i and destPath for the closure (new variable per iteration in Go 1.22+).
		idx, dst := i, destPath
		pr := newProgressReader(f.Body, partTotal, func(written, total int64) {
			stream.Write(schema.UploadFileEvent, schema.UploadFile{
				Index:   idx,
				Path:    dst,
				Written: written,
				Bytes:   total,
			})
		})

		contentType := resolveContentType(f.ContentType, "", filepath.Ext(filename))
		meta := mergeMeta(sharedMeta, extractMeta(http.Header(f.Header)))

		obj, err := mgr.CreateObject(r.Context(), name, schema.CreateObjectRequest{
			Path:        destPath,
			Body:        pr,
			ContentType: contentType,
			Meta:        meta,
		})
		if err != nil {
			rbErr := rollback(err)
			stream.Write(schema.UploadErrorEvent, schema.UploadError{
				Index:   i,
				Path:    destPath,
				Message: rbErr.Error(),
			})
			return stream.Close()
		}

		results = append(results, *obj)
		totalWritten += obj.Size
		stream.Write(schema.UploadCompleteEvent, obj)
	}

	stream.Write(schema.UploadDoneEvent, schema.UploadDone{Files: len(results), Bytes: totalWritten})
	return stream.Close()
}
