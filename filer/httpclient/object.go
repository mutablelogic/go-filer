package httpclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"strings"
	"time"

	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListObjects returns a page of the objects beneath a path in the volume
// named by the request. When there are more, Next is the key of the last
// object, which the next page starts after, with StartAfter.
func (c *Client) ListObjects(ctx context.Context, req schema.ObjectListRequest) (*schema.ObjectList, error) {
	var response schema.ObjectList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("object", req.Name), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// HeadObject returns the metadata of the object at the path in the volume,
// which is a file, a directory, or the root of the volume, when it's empty,
// from the X-Meta header of a HEAD request. Returns ErrBadParameter, without
// a request, when the path is outside the volume.
func (c *Client) HeadObject(ctx context.Context, name, key string) (*schema.Object, error) {
	opt, err := c.objectOpt(name, key)
	if err != nil {
		return nil, err
	}

	// A HEAD response has no body, so the error is only its status, which
	// is described here
	var response objectMeta
	if err := c.DoWithContext(ctx, client.MethodHead, &response, opt); err != nil {
		return nil, statusError(err, name+"/"+strings.TrimLeft(key, "/"))
	}
	return types.Ptr(response.Object), nil
}

// GetObject writes the content of the file of the request to w, and returns
// its metadata, from the X-Meta header. Its IfModifiedSince and IfNoneMatch
// conditions are sent, and when the file meets them, gofiler.ErrNotModified
// is returned, and nothing is written. A directory can't be got. The request
// isn't timed out, as a file can be large, so it's cancelled with ctx.
func (c *Client) GetObject(ctx context.Context, req schema.GetObjectRequest, w io.Writer) (*schema.Object, error) {
	if clean := strings.Trim(path.Clean("/"+req.Path), "/"); clean == "" {
		return nil, gofiler.ErrBadParameter.Withf("%s: a directory can't be got", req.Name)
	}
	opt, err := c.objectOpt(req.Name, req.Path)
	if err != nil {
		return nil, err
	}
	opts := []client.RequestOpt{opt, client.OptNoTimeout()}
	if !req.IfModifiedSince.IsZero() {
		opts = append(opts, client.OptReqHeader("If-Modified-Since", req.IfModifiedSince.UTC().Format(http.TimeFormat)))
	}
	if len(req.IfNoneMatch) > 0 {
		// An etag is quoted, unless it's *, or quoted already, or weak
		etags := make([]string, 0, len(req.IfNoneMatch))
		for _, etag := range req.IfNoneMatch {
			if etag != "*" && !strings.HasPrefix(etag, `"`) && !strings.HasPrefix(etag, `W/"`) {
				etag = `"` + etag + `"`
			}
			etags = append(etags, etag)
		}
		opts = append(opts, client.OptReqHeader("If-None-Match", strings.Join(etags, ", ")))
	}

	// A directory redirects to its listing, which isn't a file
	response := objectMeta{w: w}
	response.Path = path.Join("/", req.Path)
	if err := c.DoWithContext(ctx, client.MethodGet, &response, opts...); errors.Is(err, httpresponse.Err(http.StatusNotModified)) {
		return nil, gofiler.ErrNotModified.Withf("%s%s", req.Name, req.Path)
	} else if err != nil {
		return nil, err
	} else if response.IsDir {
		return nil, gofiler.ErrBadParameter.Withf("%s%s: a directory can't be got", req.Name, response.Path)
	}
	return types.Ptr(response.Object), nil
}

// DeleteObjects deletes the file of the request, or a directory, when it's
// recursive, and every object beneath it, and returns the objects deleted,
// which are files, not directories. Returns ErrBadParameter, without a
// request, when the path is outside the volume, or is the root.
func (c *Client) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	if clean := strings.Trim(path.Clean("/"+req.Path), "/"); clean == "" {
		return nil, gofiler.ErrBadParameter.Withf("%s: the root of a volume can't be deleted", req.Name)
	}
	opt, err := c.objectOpt(req.Name, req.Path)
	if err != nil {
		return nil, err
	}
	opts := []client.RequestOpt{opt}
	if req.Recursive {
		opts = append(opts, client.OptQuery(url.Values{"recursive": {"true"}}))
	}

	// Perform request
	var response schema.DeleteObjectsResponse
	if err := c.DoWithContext(ctx, client.MethodDelete, &response, opts...); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// PutObject uploads the request's body to the file at its path in its
// volume, creating it, and the directories it's in, or replacing it. With
// IfNotExists, a file which exists isn't replaced, and with IfExists, a file
// which doesn't exist isn't created, which are gofiler.ErrPreconditionFailed.
// The file's modified time is ModTime, when it's set, and a nil body is an
// empty file. Returns the file. The request isn't timed out, as a file can
// be large, so it's cancelled with ctx.
func (c *Client) PutObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	if clean := strings.Trim(path.Clean("/"+req.Path), "/"); clean == "" {
		return nil, gofiler.ErrBadParameter.Withf("%s: the root of a volume can't be uploaded to", req.Name)
	}
	opt, err := c.objectOpt(req.Name, req.Path)
	if err != nil {
		return nil, err
	}
	opts := []client.RequestOpt{opt, client.OptNoTimeout()}
	if req.IfNotExists {
		opts = append(opts, client.OptReqHeader("If-None-Match", "*"))
	}
	if req.IfExists {
		opts = append(opts, client.OptReqHeader("If-Match", "*"))
	}
	if !req.ModTime.IsZero() {
		opts = append(opts, client.OptReqHeader(types.ContentModifiedHeader, req.ModTime.UTC().Format(http.TimeFormat)))
	}
	contentType := req.ContentType
	if contentType == "" {
		contentType = types.ContentTypeBinary
	}
	body := req.Body
	if body == nil {
		body = strings.NewReader("")
	}

	// Perform request
	var response schema.Object
	if err := c.DoWithContext(ctx, &bodyPayload{Reader: body, contentType: contentType}, &response, opts...); err != nil {
		var code httpresponse.Err
		var resp httpresponse.ErrResponse
		if (errors.As(err, &code) && code == httpresponse.Err(http.StatusPreconditionFailed)) || (errors.As(err, &resp) && resp.Code == http.StatusPreconditionFailed) {
			if req.IfNotExists {
				return nil, gofiler.ErrPreconditionFailed.Withf("%s%s exists", req.Name, req.Path)
			}
			return nil, gofiler.ErrPreconditionFailed.Withf("%s%s doesn't exist", req.Name, req.Path)
		}
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// UploadFile is a file to upload into a directory, by its path in it, which
// can have directories, which is opened as it's uploaded
type UploadFile struct {
	Path    string
	ModTime time.Time // the file's modification time, when it isn't zero
	Open    func() (io.ReadCloser, error)
}

// UploadObjects uploads the files into the directory at the path in the
// volume, or its root, when it's empty, in one request, whose multipart form
// is written as it's sent, opening each file as it is, so they aren't
// buffered. A file which exists is skipped, unless overwrite is set, as is
// one which can't be stored, such as a hidden file. Returns the files which
// were written, and those which were skipped, and why. The request isn't
// timed out, as the files can be large, so it's cancelled with ctx.
func (c *Client) UploadObjects(ctx context.Context, name, dir string, files []UploadFile, overwrite bool) (*schema.ObjectUploadResponse, error) {
	opt, err := c.objectOpt(name, dir)
	if err != nil {
		return nil, err
	}
	opts := []client.RequestOpt{opt, client.OptNoTimeout()}
	if overwrite {
		opts = append(opts, client.OptQuery(url.Values{"overwrite": {"true"}}))
	}

	// Write the form to the request as it's sent, closing it with the error
	// opening or reading a file, which stops the request
	pr, pw := io.Pipe()
	form := multipart.NewWriter(pw)
	go func() {
		pw.CloseWithError(writeForm(form, files))
	}()
	defer pr.Close()

	// Perform request
	var response schema.ObjectUploadResponse
	payload := &formPayload{Reader: pr, contentType: form.FormDataContentType()}
	if err := c.DoWithContext(ctx, payload, &response, opts...); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// writeForm writes each file to the form, as a "file" part, whose file name
// is its path, and then closes the form
func writeForm(form *multipart.Writer, files []UploadFile) error {
	for _, file := range files {
		header := textproto.MIMEHeader{}
		header.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": "file", "filename": file.Path}))
		header.Set("Content-Type", types.ContentTypeBinary)
		if !file.ModTime.IsZero() {
			header.Set(types.ContentModifiedHeader, file.ModTime.UTC().Format(http.TimeFormat))
		}
		w, err := form.CreatePart(header)
		if err != nil {
			return err
		}
		r, err := file.Open()
		if err != nil {
			return err
		}
		_, err = io.Copy(w, r)
		if err := errors.Join(err, r.Close()); err != nil {
			return err
		}
	}
	return form.Close()
}

// objectOpt returns the option of the path of the object at the path in the
// volume, whose elements are each escaped. The root of the volume is
// object/<name>/, with a "/", which object/<name> without it, the listing of
// the volume, isn't.
func (c *Client) objectOpt(name, key string) (client.RequestOpt, error) {
	clean := path.Clean(strings.TrimLeft(key, "/"))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return nil, gofiler.ErrBadParameter.Withf("path is outside the volume: %q", key)
	} else if clean == "." {
		endpoint, err := url.JoinPath(c.endpoint, "object", name+"/")
		if err != nil {
			return nil, err
		}
		return client.OptReqEndpoint(endpoint), nil
	}
	elems := []any{"object", name}
	for _, elem := range strings.Split(clean, "/") {
		elems = append(elems, elem)
	}
	return client.OptPath(elems...), nil
}

// formPayload is the body of a request to upload files, which is a
// multipart form
type formPayload struct {
	io.Reader
	contentType string
}

func (*formPayload) Method() string { return http.MethodPost }
func (*formPayload) Accept() string { return types.ContentTypeJSON }
func (p *formPayload) Type() string { return p.contentType }

// statusError returns the error of a response without a body, such as a
// HEAD's, whose status is all there is, as the filer's error, with what was
// requested, so 404 Not Found is "not found: media/a.txt"
func statusError(err error, location string) error {
	var code httpresponse.Err
	if !errors.As(err, &code) {
		return err
	}
	switch int(code) {
	case http.StatusBadRequest:
		return gofiler.ErrBadParameter.With(location)
	case http.StatusForbidden:
		return gofiler.ErrForbidden.With(location)
	case http.StatusNotFound:
		return gofiler.ErrNotFound.With(location)
	case http.StatusConflict:
		return gofiler.ErrConflict.With(location)
	case http.StatusPreconditionFailed:
		return gofiler.ErrPreconditionFailed.With(location)
	case http.StatusServiceUnavailable:
		return gofiler.ErrServiceUnavailable.With(location)
	default:
		return fmt.Errorf("%s: %w", location, err)
	}
}

// bodyPayload is the body of a request to upload a file, of its type
type bodyPayload struct {
	io.Reader
	contentType string
}

func (*bodyPayload) Method() string { return http.MethodPut }
func (*bodyPayload) Accept() string { return types.ContentTypeJSON }
func (p *bodyPayload) Type() string { return p.contentType }

// objectMeta is the response with an object's metadata in the X-Meta header,
// which writes its content to w, unless it's nil
type objectMeta struct {
	schema.Object
	w io.Writer
}

func (r *objectMeta) Unmarshal(header http.Header, body io.Reader) error {
	if meta := header.Get(types.ContentMetaHeader); meta == "" && r.w != nil {
		// A directory redirects to its listing, which has no metadata
		return gofiler.ErrBadParameter.Withf("%s is a directory, not a file", r.Path)
	} else if meta == "" {
		return fmt.Errorf("missing %s header", types.ContentMetaHeader)
	} else if err := json.Unmarshal([]byte(meta), &r.Object); err != nil {
		return fmt.Errorf("invalid %s header: %w", types.ContentMetaHeader, err)
	}
	if r.w == nil || r.IsDir {
		return nil
	}
	_, err := io.Copy(r.w, body)
	return err
}
