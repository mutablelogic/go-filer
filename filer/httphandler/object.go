package httphandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	manager "github.com/mutablelogic/go-filer/filer/manager"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// HANDLER IMPLEMENTATIONS

func objectList(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	var request schema.ListObjectsRequest

	if err := httprequest.Query(r.URL.Query(), &request); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	request.Path = types.NormalisePath(request.Path)

	response, err := mgr.ListObjects(r.Context(), r.PathValue("name"), request)
	if err != nil {
		return httpresponse.Error(w, err)
	}
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func objectDeleteRoot(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	var params struct {
		Recursive bool `json:"recursive,omitempty"`
	}
	if err := httprequest.Query(r.URL.Query(), &params); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	resp, err := mgr.DeleteObjects(r.Context(), r.PathValue("name"), schema.DeleteObjectsRequest{
		Path:      "/",
		Recursive: params.Recursive,
	})
	if err != nil {
		return httpresponse.Error(w, err)
	}
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), resp)
}

func objectPut(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	req := schema.CreateObjectRequest{
		Path: types.NormalisePath(r.PathValue("path")),
		Body: r.Body,
	}

	if ct := r.Header.Get(types.ContentTypeHeader); ct != "" {
		req.ContentType = ct
	}
	if lm := r.Header.Get(types.ContentModifiedHeader); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			req.ModTime = t
		}
	}
	req.Meta = extractMeta(r.Header)
	if r.Header.Get("If-None-Match") == "*" {
		req.IfNotExists = true
	}

	obj, err := mgr.CreateObject(r.Context(), r.PathValue("name"), req)
	if err != nil {
		return httpresponse.Error(w, err)
	}
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), obj)
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

	reader, _, err := mgr.ReadObject(r.Context(), name, schema.ReadObjectRequest{
		GetObjectRequest: schema.GetObjectRequest{Path: path},
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

func objectDelete(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	path := types.NormalisePath(r.PathValue("path"))

	if r.URL.Query().Has("recursive") {
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

	obj, err := mgr.DeleteObject(r.Context(), r.PathValue("name"), schema.DeleteObjectRequest{Path: path})
	if err != nil {
		return httpresponse.Error(w, err)
	}
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), obj)
}

func objectUpload(w http.ResponseWriter, r *http.Request, mgr *manager.Manager) error {
	mr, err := r.MultipartReader()
	if err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}

	maxFiles := schema.MaxUploadFiles
	resultsCap := 0
	if declared := strings.TrimSpace(r.Header.Get("X-Upload-Count")); declared != "" {
		n, err := strconv.Atoi(declared)
		if err != nil || n < 0 {
			return httpresponse.Error(w, httpresponse.ErrBadRequest.Withf("invalid X-Upload-Count value %q", declared))
		}
		if maxFiles > 0 && n > maxFiles {
			return httpresponse.Error(w, httpresponse.Err(http.StatusRequestEntityTooLarge).Withf(
				"too many files in upload (%d > %d limit)", n, maxFiles,
			))
		}
		resultsCap = n
	}

	sharedMeta := extractMeta(r.Header)
	basePath := types.NormalisePath(r.PathValue("path"))
	name := r.PathValue("name")
	isDir := basePath == "/" || strings.HasSuffix(r.URL.Path, "/")

	var spanErr error
	ctx, endSpan := otel.StartSpan(mgr.Tracer(), r.Context(), "filer.handler.Upload",
		attribute.String("name", name),
		attribute.String("path", basePath),
	)
	defer func() { endSpan(spanErr) }()

	results := make([]schema.Object, 0, resultsCap)
	uploadedPaths := make([]string, 0, resultsCap)
	rollback := func(uploadErr error) error {
		var errs []error
		for _, p := range uploadedPaths {
			rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer rollbackCancel()
			if _, err := mgr.DeleteObject(rollbackCtx, name, schema.DeleteObjectRequest{Path: p}); err != nil {
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
		if maxFiles > 0 && fileCount > maxFiles {
			return httpresponse.Error(w, rollback(httpresponse.Err(http.StatusRequestEntityTooLarge).Withf(
				"too many files in upload (%d > %d limit)", fileCount, maxFiles,
			)))
		}
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

		obj, err := mgr.CreateObject(ctx, name, buildCreateRequest(f, sharedMeta, destPath, part))
		if err != nil {
			spanErr = err
			return httpresponse.Error(w, rollback(err))
		}
		uploadedPaths = append(uploadedPaths, obj.Path)
		results = append(results, *obj)
	}

	if fileCount == 0 {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(`missing or unreadable "file" form field`))
	}

	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), results)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE HELPERS

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

func checkPreconditions(w http.ResponseWriter, r *http.Request, obj *schema.Object) bool {
	etag := obj.ETag
	modtime := obj.ModTime

	if im := r.Header.Get("If-Match"); im != "" {
		if !matchETags(im, etag, true) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
	} else if ius := r.Header.Get("If-Unmodified-Since"); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && modtime.After(t) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
	}

	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if matchETags(inm, etag, false) {
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	} else if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !modtime.After(t) {
			w.WriteHeader(http.StatusNotModified)
			return true
		}
	}

	return false
}

func matchETags(header, etag string, strong bool) bool {
	if strings.TrimSpace(header) == "*" {
		return etag != ""
	}
	if strong && strings.HasPrefix(etag, "W/") {
		return false
	}
	for _, part := range strings.Split(header, ",") {
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

func buildCreateRequest(f types.File, sharedMeta schema.ObjectMeta, destPath string, body io.Reader) schema.CreateObjectRequest {
	contentType := resolveContentType(f.ContentType, "", filepath.Ext(f.Path))
	meta := mergeMeta(sharedMeta, extractMeta(http.Header(f.Header)))
	req := schema.CreateObjectRequest{
		Path:        destPath,
		Body:        body,
		ContentType: contentType,
		Meta:        meta,
	}
	if lm := f.Header.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			req.ModTime = t
		}
	}
	return req
}

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
