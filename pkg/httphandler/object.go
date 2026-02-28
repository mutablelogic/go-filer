package httphandler

import (
	"encoding/json"
	"errors"
	"fmt"
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

	// If-None-Match: * means "create only if the object does not already exist" (RFC 7232 §3.2).
	if r.Header.Get("If-None-Match") == "*" {
		req.IfNotExists = true
	}

	// Create the object in the manager
	obj, err := mgr.CreateObject(r.Context(), r.PathValue("name"), req)
	if err != nil {
		return httpresponse.Error(w, err)
	}

	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), obj)
}

func objectDelete(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	path := types.NormalisePath(r.PathValue("path"))

	// When ?recursive is present (regardless of true/false value) use bulk-delete semantics:
	//   ?recursive=true  — remove the full sub-tree under path
	//   ?recursive=false — remove only the immediate children of path (non-recursive bulk delete)
	if r.URL.Query().Has("recursive") {
		// Only parse the boolean flag from the query — Path always comes from the URL.
		var params struct {
			Recursive bool `json:"recursive,omitempty"`
		}
		if err := httprequest.Query(r.URL.Query(), &params); err != nil {
			return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
		}
		resp, err := mgr.DeleteObjects(r.Context(), r.PathValue("name"), schema.DeleteObjectsRequest{
			Path:      path,
			Recursive: params.Recursive,
		})
		if err != nil {
			return httpresponse.Error(w, err)
		}
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
	}

	// No ?recursive param — single object delete.
	obj, err := mgr.DeleteObject(r.Context(), r.PathValue("name"), schema.DeleteObjectRequest{Path: path})
	if err != nil {
		return httpresponse.Error(w, err)
	}
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), obj)
}

func objectUpload(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	// For SSE: stream each multipart part directly to the backend one-at-a-time,
	// avoiding ParseMultipartForm which buffers the entire body to memory or /tmp (tmpfs).
	if accept, _ := types.AcceptContentType(r); accept == types.ContentTypeTextStream {
		return objectUploadSSEStream(w, r, mgr)
	}
	// Non-SSE: still stream multipart parts directly to the backend to avoid
	// buffering large request bodies in memory or /tmp.
	return objectUploadJSONStream(w, r, mgr)
}

func objectHead(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	_, _, stop, err := fetchObjectMeta(w, r, mgr)
	if err != nil || stop {
		return err
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func objectGet(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	path := types.NormalisePath(r.PathValue("path"))
	ext := filepath.Ext(r.PathValue("path"))
	name := r.PathValue("name")

	obj, contentType, stop, err := fetchObjectMeta(w, r, mgr)
	if err != nil || stop {
		return err
	}

	// Preconditions passed: now open the body stream.
	reader, _, err := mgr.ReadObject(r.Context(), name, schema.ReadObjectRequest{
		GetObjectRequest: schema.GetObjectRequest{Path: path},
	})
	if err != nil {
		return httpresponse.Error(w, err)
	}
	defer reader.Close()

	// Sniff the first 512 bytes to improve Content-Type detection when the
	// stored type is absent or the generic binary fallback.
	buffer := make([]byte, 512)
	n, err := io.ReadFull(reader, buffer)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return httpresponse.Error(w, err)
	}

	// Update Content-Type before status is committed (headers still mutable).
	sniffed := http.DetectContentType(buffer[:n])
	if improved := resolveContentType(obj.ContentType, sniffed, ext); improved != contentType {
		w.Header().Set(types.ContentTypeHeader, improved)
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

// fetchObjectMeta fetches object metadata, writes response headers, and
// evaluates RFC 7232 conditional request headers. It returns the object, the
// resolved content-type, and stop=true if a precondition response (304/412)
// has already been committed. Callers must return immediately when stop is true.
func fetchObjectMeta(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) (*schema.Object, string, bool, error) {
	obj, err := mgr.GetObject(r.Context(), r.PathValue("name"), schema.GetObjectRequest{
		Path: types.NormalisePath(r.PathValue("path")),
	})
	if err != nil {
		return nil, "", true, httpresponse.Error(w, err)
	}
	contentType := resolveContentType(obj.ContentType, "", filepath.Ext(r.PathValue("path")))
	writeObjectHeaders(w, obj, contentType)
	if checkPreconditions(w, r, obj) {
		return obj, contentType, true, nil
	}
	return obj, contentType, false, nil
}

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

// resolveContentType returns the best content-type for an object. Priority:
//  1. Explicitly-stored type (unless it is the generic binary fallback)
//  2. Sniffed type from the body prefix (unless it is the generic binary fallback)
//  3. MIME type inferred from the file extension
//  4. The stored type as-is (at this point it can only be application/octet-stream or empty)
//  5. The hardcoded binary fallback
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
	// stored is either application/octet-stream or empty; return it when set
	// so we propagate the caller's explicit value rather than re-hardcoding it.
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
// of quoted ETags) matches etag. strong=true requires strong comparison (RFC 7232
// §2.3.2): both the stored ETag and each header ETag must be strong (no W/ prefix).
// strong=false allows weak comparison (W/ prefix stripped before comparing).
func matchETags(header, etag string, strong bool) bool {
	if strings.TrimSpace(header) == "*" {
		return etag != ""
	}
	// For strong comparison a weak stored ETag never matches any header value.
	if strong && strings.HasPrefix(etag, "W/") {
		return false
	}
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if strong && strings.HasPrefix(part, "W/") {
			continue // weak header ETag never satisfies strong comparison
		}
		if strings.Trim(strings.TrimPrefix(part, "W/"), `"`) ==
			strings.Trim(strings.TrimPrefix(etag, "W/"), `"`) {
			return true
		}
	}
	return false
}

// buildCreateRequest assembles a CreateObjectRequest from a multipart file part,
// shared metadata, and the resolved destination path. body overrides f.Body so
// the caller can wrap it (e.g. in a progressReader) before passing it here.
func buildCreateRequest(f types.File, sharedMeta schema.ObjectMeta, destPath string, body io.Reader) schema.CreateObjectRequest {
	contentType := resolveContentType(f.ContentType, "", filepath.Ext(f.Path))
	meta := mergeMeta(sharedMeta, extractMeta(http.Header(f.Header)))
	req := schema.CreateObjectRequest{
		Path:        destPath,
		Body:        body,
		ContentType: contentType,
		Meta:        meta,
	}
	// Preserve the file modification time when the part carries a Last-Modified header.
	if lm := f.Header.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			req.ModTime = t
		}
	}
	return req
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

// objectUploadSSEStream handles an SSE multipart upload by reading parts one at
// a time via r.MultipartReader, streaming each directly to the backend without
// first buffering the entire request body.  This avoids the OOM / hang caused
// by ParseMultipartForm spilling large uploads to /tmp (a RAM-backed tmpfs
// inside Docker containers).
//
// The caller should set the X-Upload-Count request header to the number of
// file parts when it knows the count in advance; the value is forwarded in the
// UploadStart event so the client can render accurate progress bars.
//
// Event sequence:
//
//	start    — once, before the loop; payload: schema.UploadStart
//	file     — written==0 on first emission (file started); then every 64 KiB; payload: schema.UploadFile
//	complete — after each file is committed; payload: schema.Object
//	error    — on failure, after rollback; payload: schema.UploadError; stream closed immediately after
//	done     — all files committed successfully; payload: schema.UploadDone
func objectUploadSSEStream(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	// Allow interleaved reads from the request body and writes to the
	// response. Without this, Go's HTTP/1.1 server stalls response
	// flushes until the request body is fully consumed, deadlocking the
	// SSE event channel.
	if rc := http.NewResponseController(w); rc != nil {
		_ = rc.EnableFullDuplex() // ignore error for httptest.ResponseRecorder
	}

	mr, err := r.MultipartReader()
	if err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}

	sharedMeta := extractMeta(r.Header)
	basePath := types.NormalisePath(r.PathValue("path"))
	name := r.PathValue("name")
	isDir := basePath == "/" || strings.HasSuffix(r.URL.Path, "/")

	// X-Upload-Count lets the client declare how many file parts to expect so
	// the progress UI can show "file 1 of N".
	var fileCount int
	if v := r.Header.Get("X-Upload-Count"); v != "" {
		if n, err2 := strconv.Atoi(v); err2 == nil {
			fileCount = n
		}
	}

	// Open the SSE stream — commits 200 OK; no HTTP errors are possible after this point.
	// X-Accel-Buffering tells nginx to disable response buffering for this
	// request so SSE events are flushed to the client immediately.
	w.Header().Set("X-Accel-Buffering", "no")
	stream := httpresponse.NewTextStream(w)
	stream.Write(schema.UploadStartEvent, schema.UploadStart{Files: fileCount})

	results := make([]schema.Object, 0)
	var (
		totalWritten int64
		index        int
	)

	rollback := func(uploadErr error) error {
		var errs []error
		for _, obj := range results {
			if _, err := mgr.DeleteObject(r.Context(), name, schema.DeleteObjectRequest{Path: obj.Path}); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(append([]error{uploadErr}, errs...)...)
	}

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			rbErr := rollback(err)
			stream.Write(schema.UploadErrorEvent, schema.UploadError{
				Index:   index,
				Message: rbErr.Error(),
			})
			return stream.Close()
		}

		// Skip non-file form fields (e.g. plain text inputs).
		if part.FormName() != "file" {
			continue
		}

		// The multipart encoder stores only the basename in Content-Disposition
		// (per RFC 7578 §4.2) and preserves the full relative path in the
		// X-Path part header when directory components are present.
		filename := part.Header.Get(types.ContentPathHeader)
		if filename == "" {
			filename = part.FileName()
		}
		if filename == "" {
			filename = fmt.Sprintf("file-%d", index)
		}

		destPath := basePath
		if isDir {
			destPath = types.JoinPath(basePath, filename)
		}

		// Wrap part in a types.File so buildCreateRequest can extract metadata.
		// Body is io.NopCloser because *multipart.Part only implements io.Reader.
		f := types.File{
			Path:        filename,
			Body:        io.NopCloser(part),
			ContentType: part.Header.Get("Content-Type"),
			Header:      part.Header,
		}

		var partTotal int64
		if v := f.Header.Get(types.ContentLengthHeader); v != "" {
			partTotal, _ = strconv.ParseInt(v, 10, 64)
		}

		// Emit file-start event (Written == 0 signals the file has begun).
		stream.Write(schema.UploadFileEvent, schema.UploadFile{
			Index:   index,
			Path:    destPath,
			Written: 0,
			Bytes:   partTotal,
		})

		// Wrap part in a progress reader so we emit progress events; pass pr as
		// the body so buildCreateRequest uses it rather than f.Body.
		idx, dst := index, destPath
		pr := newProgressReader(part, partTotal, func(written, total int64) {
			stream.Write(schema.UploadFileEvent, schema.UploadFile{
				Index:   idx,
				Path:    dst,
				Written: written,
				Bytes:   total,
			})
		})

		obj, err := mgr.CreateObject(r.Context(), name, buildCreateRequest(f, sharedMeta, destPath, pr))
		if err != nil {
			rbErr := rollback(err)
			stream.Write(schema.UploadErrorEvent, schema.UploadError{
				Index:   index,
				Path:    destPath,
				Message: rbErr.Error(),
			})
			return stream.Close()
		}

		results = append(results, *obj)
		totalWritten += obj.Size
		stream.Write(schema.UploadCompleteEvent, obj)
		index++
	}

	// Drain any trailing data from the request body (e.g. the multipart
	// epilogue after the final boundary) so the HTTP/1.1 connection can be
	// safely reused for keep-alive.
	_, _ = io.Copy(io.Discard, r.Body)

	stream.Write(schema.UploadDoneEvent, schema.UploadDone{Files: len(results), Bytes: totalWritten})
	return stream.Close()
}

// objectUploadJSONStream handles multipart uploads without SSE by streaming
// each file part directly to the backend and returning a JSON array on success.
// This avoids buffering the entire request body in memory/tmpfs.
func objectUploadJSONStream(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	mr, err := r.MultipartReader()
	if err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}

	sharedMeta := extractMeta(r.Header)
	basePath := types.NormalisePath(r.PathValue("path"))
	name := r.PathValue("name")
	isDir := basePath == "/" || strings.HasSuffix(r.URL.Path, "/")

	results := make([]schema.Object, 0)
	rollback := func(uploadErr error) error {
		var errs []error
		for _, obj := range results {
			if _, err := mgr.DeleteObject(r.Context(), name, schema.DeleteObjectRequest{Path: obj.Path}); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(append([]error{uploadErr}, errs...)...)
	}

	fileCount := 0
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return httpresponse.Error(w, rollback(err))
		}

		if part.FormName() != "file" {
			continue
		}

		fileCount++
		if !isDir && fileCount > 1 {
			return httpresponse.Error(w, rollback(httpresponse.ErrBadRequest.Withf(
				"cannot upload %d files to explicit path %q; add a trailing slash to upload to a directory",
				fileCount, basePath,
			)))
		}

		filename := part.Header.Get(types.ContentPathHeader)
		if filename == "" {
			filename = part.FileName()
		}
		if filename == "" {
			filename = fmt.Sprintf("file-%d", fileCount-1)
		}

		destPath := basePath
		if isDir {
			destPath = types.JoinPath(basePath, filename)
		}

		f := types.File{
			Path:        filename,
			Body:        io.NopCloser(part),
			ContentType: part.Header.Get(types.ContentTypeHeader),
			Header:      part.Header,
		}

		obj, err := mgr.CreateObject(r.Context(), name, buildCreateRequest(f, sharedMeta, destPath, part))
		if err != nil {
			return httpresponse.Error(w, rollback(err))
		}
		results = append(results, *obj)
	}

	if fileCount == 0 {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(`missing or unreadable "file" form field`))
	}

	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), results)
}
