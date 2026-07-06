package httpclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type getObjectResponse struct {
	Object *schema.Object
}

type readObjectResponse struct {
	Object *schema.Object
	metaFn func(*schema.Object) error
	fn     func([]byte) error
}

type putObjectPayload struct {
	body        io.Reader
	contentType string
}

type putObjectOptions struct {
	contentType string
	ifNotExists bool
}

type PutObjectOpt func(*putObjectOptions)

var _ client.Unmarshaler = (*getObjectResponse)(nil)
var _ client.Unmarshaler = (*readObjectResponse)(nil)
var _ client.Payload = (*putObjectPayload)(nil)

func PutObjectContentType(contentType string) PutObjectOpt {
	return func(opts *putObjectOptions) {
		opts.contentType = contentType
	}
}

func PutObjectIfNotExists() PutObjectOpt {
	return func(opts *putObjectOptions) {
		opts.ifNotExists = true
	}
}

func (r *getObjectResponse) Unmarshal(header http.Header, _ io.Reader) error {
	data := header.Get(schema.ContentObjectHeader)
	if data == "" {
		return fmt.Errorf("missing %s header", schema.ContentObjectHeader)
	}
	var obj schema.Object
	if err := json.Unmarshal([]byte(data), &obj); err != nil {
		return err
	}
	r.Object = &obj
	return nil
}

func (r *readObjectResponse) Unmarshal(header http.Header, reader io.Reader) error {
	data := header.Get(schema.ContentObjectHeader)
	if data == "" {
		return fmt.Errorf("missing %s header", schema.ContentObjectHeader)
	}
	var obj schema.Object
	if err := json.Unmarshal([]byte(data), &obj); err != nil {
		return err
	}
	r.Object = &obj
	if r.metaFn != nil {
		if err := r.metaFn(r.Object); err != nil {
			return err
		}
	}

	buf := make([]byte, 32*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if callErr := r.fn(buf[:n]); callErr != nil {
				return callErr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (p *putObjectPayload) Method() string {
	return http.MethodPut
}

func (p *putObjectPayload) Accept() string {
	return types.ContentTypeJSON
}

func (p *putObjectPayload) Type() string {
	if p.contentType != "" {
		return p.contentType
	}
	return types.ContentTypeBinary
}

func (p *putObjectPayload) Read(data []byte) (int, error) {
	return p.body.Read(data)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) GetObject(ctx context.Context, volume, path string) (*schema.Object, error) {
	var response getObjectResponse
	if err := c.DoWithContext(ctx,
		client.NewRequestEx(http.MethodHead, ""),
		&response,
		client.OptPath("object", volume, path),
	); err != nil {
		return nil, err
	}
	return response.Object, nil
}

func (c *Client) ReadObject(ctx context.Context, volume, path string, attr schema.ObjectAttr, fn func([]byte) error) (*schema.Object, error) {
	return c.ReadObjectWithMeta(ctx, volume, path, attr, nil, fn)
}

func (c *Client) ReadObjectWithMeta(ctx context.Context, volume, path string, attr schema.ObjectAttr, metaFn func(*schema.Object) error, fn func([]byte) error) (*schema.Object, error) {
	if fn == nil {
		return nil, gofiler.ErrBadParameter.With("callback is required")
	}

	reqOpts := []client.RequestOpt{client.OptPath("object", volume, path)}
	if etag := types.Value(attr.ETag); etag != "" {
		reqOpts = append(reqOpts, client.OptReqHeader(schema.ContentIfNoneMatchHeader, etag))
	}
	if !attr.ModTime.IsZero() {
		reqOpts = append(reqOpts, client.OptReqHeader(schema.ContentIfModifiedSinceHeader, attr.ModTime.Format(http.TimeFormat)))
	}

	var response readObjectResponse
	response.metaFn = metaFn
	response.fn = fn
	if err := c.DoWithContext(ctx, client.NewRequestEx(http.MethodGet, ""), &response, reqOpts...); err != nil {
		if errors.Is(err, httpresponse.Err(http.StatusNotModified)) {
			object, getErr := c.GetObject(ctx, volume, path)
			if getErr != nil {
				return nil, getErr
			}
			return object, err
		}
		return nil, err
	}
	return response.Object, nil
}

func (c *Client) PutObject(ctx context.Context, volume, path string, r io.Reader, attr schema.ObjectAttr, opts ...PutObjectOpt) (*schema.Object, error) {
	if r == nil {
		return nil, gofiler.ErrBadParameter.With("reader is required")
	}
	var putOpts putObjectOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&putOpts)
		}
	}

	reqOpts := []client.RequestOpt{client.OptPath("object", volume, path)}
	if attr.Size != nil {
		reqOpts = append(reqOpts, client.OptReqHeader(types.ContentLengthHeader, strconv.FormatInt(types.Value(attr.Size), 10)))
	}
	if etag := types.Value(attr.ETag); etag != "" {
		reqOpts = append(reqOpts, client.OptReqHeader(types.ContentHashHeader, etag))
	}
	if !attr.ModTime.IsZero() {
		reqOpts = append(reqOpts, client.OptReqHeader(types.ContentModifiedHeader, attr.ModTime.Format(http.TimeFormat)))
	}
	if putOpts.ifNotExists {
		reqOpts = append(reqOpts, client.OptReqHeader(schema.ContentIfNoneMatchHeader, "*"))
	}

	var response schema.Object
	if err := c.DoWithContext(ctx, &putObjectPayload{body: r, contentType: putOpts.contentType}, &response, reqOpts...); err != nil {
		return nil, err
	}
	return types.Ptr(response), nil
}

func (c *Client) ListObjects(ctx context.Context, req schema.ObjectListRequest) (*schema.ObjectList, error) {
	var response schema.ObjectList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("object"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
