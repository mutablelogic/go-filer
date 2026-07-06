package httpclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type getObjectResponse struct {
	Object *schema.Object
}

type putObjectPayload struct {
	body        io.Reader
	contentType string
}

var _ client.Unmarshaler = (*getObjectResponse)(nil)
var _ client.Payload = (*putObjectPayload)(nil)

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

func (c *Client) PutObject(ctx context.Context, volume, path string, r io.Reader, attr schema.ObjectAttr, contentType ...string) (*schema.Object, error) {
	if r == nil {
		return nil, gofiler.ErrBadParameter.With("reader is required")
	}

	opts := []client.RequestOpt{client.OptPath("object", volume, path)}
	if attr.Size != nil {
		opts = append(opts, client.OptReqHeader(types.ContentLengthHeader, strconv.FormatInt(types.Value(attr.Size), 10)))
	}
	if etag := types.Value(attr.ETag); etag != "" {
		opts = append(opts, client.OptReqHeader(types.ContentHashHeader, etag))
	}
	if !attr.ModTime.IsZero() {
		opts = append(opts, client.OptReqHeader(types.ContentModifiedHeader, attr.ModTime.Format(http.TimeFormat)))
	}
	typ := ""
	if len(contentType) > 0 {
		typ = contentType[0]
	}

	var response schema.Object
	if err := c.DoWithContext(ctx, &putObjectPayload{body: r, contentType: typ}, &response, opts...); err != nil {
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
