package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// putPayload implements client.Payload for PUT requests with a body.
type putPayload struct {
	body        io.Reader
	contentType string
}

// readObjectResponse holds the streamed response from a ReadObject request.
type readObjectResponse struct {
	Body   io.ReadCloser
	Object *schema.Object
}

// getObjectResponse holds the metadata-only response from a GetObject (HEAD) request.
type getObjectResponse struct {
	Object *schema.Object
}

// Ensure putPayload implements client.Payload
var _ client.Payload = (*putPayload)(nil)

// Ensure readObjectResponse implements client.Unmarshaler
var _ client.Unmarshaler = (*readObjectResponse)(nil)

// Ensure getObjectResponse implements client.Unmarshaler
var _ client.Unmarshaler = (*getObjectResponse)(nil)

func (p *putPayload) Method() string { return http.MethodPut }
func (p *putPayload) Accept() string { return "application/json" }
func (p *putPayload) Type() string {
	if p.contentType != "" {
		return p.contentType
	}
	return "application/octet-stream"
}
func (p *putPayload) Read(b []byte) (int, error) { return p.body.Read(b) }

func (r *readObjectResponse) Unmarshal(header http.Header, reader io.Reader) error {
	if metaJSON := header.Get(schema.ObjectMetaHeader); metaJSON != "" {
		var obj schema.Object
		if err := json.Unmarshal([]byte(metaJSON), &obj); err != nil {
			return err
		}
		r.Object = &obj
	}
	// go-client closes resp.Body after Unmarshal returns, so buffer now.
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, reader); err != nil {
		return err
	}
	r.Body = io.NopCloser(&buf)
	return nil
}

func (r *getObjectResponse) Unmarshal(header http.Header, _ io.Reader) error {
	if metaJSON := header.Get(schema.ObjectMetaHeader); metaJSON != "" {
		var obj schema.Object
		if err := json.Unmarshal([]byte(metaJSON), &obj); err != nil {
			return err
		}
		r.Object = &obj
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListObjects returns a list of objects at the given backend name and optional path prefix.
func (c *Client) ListObjects(ctx context.Context, name string, req schema.ListObjectsRequest) (*schema.ListObjectsResponse, error) {
	query := make(url.Values)
	// Path is a filter prefix, not a URL path segment — always navigate to /{name}
	// and pass path as a query parameter so the server routes to the list handler.
	if req.Path != "" {
		query.Set("path", req.Path)
	}
	if req.Recursive {
		query.Set("recursive", "true")
	}
	if req.Offset > 0 {
		query.Set("offset", strconv.Itoa(req.Offset))
	}
	if req.Limit > 0 {
		query.Set("limit", strconv.Itoa(req.Limit))
	}
	var response schema.ListObjectsResponse
	if err := c.DoWithContext(ctx, client.NewRequest(), &response,
		client.OptPath(name),
		client.OptQuery(query),
	); err != nil {
		return nil, err
	}
	return &response, nil
}

// CreateObject uploads content using PUT, forwarding ContentType, ModTime and Meta as request headers.
func (c *Client) CreateObject(ctx context.Context, name string, req schema.CreateObjectRequest) (*schema.Object, error) {
	payload := &putPayload{body: req.Body, contentType: req.ContentType}

	// Build per-request header opts from the request metadata
	opts := []client.RequestOpt{client.OptPath(name, req.Path)}
	if !req.ModTime.IsZero() {
		opts = append(opts, client.OptReqHeader("Last-Modified", req.ModTime.Format(http.TimeFormat)))
	}
	// If-None-Match: * triggers the server's IfNotExists check (RFC 7232 §3.2).
	if req.IfNotExists {
		opts = append(opts, client.OptReqHeader("If-None-Match", "*"))
	}
	for k, v := range req.Meta {
		opts = append(opts, client.OptReqHeader(http.CanonicalHeaderKey(schema.ObjectMetaKeyPrefix+k), v))
	}

	var response schema.Object
	if err := c.DoWithContext(ctx, payload, &response, opts...); err != nil {
		return nil, err
	}
	return &response, nil
}

// GetObject retrieves metadata only for an object using HEAD (no body download).
func (c *Client) GetObject(ctx context.Context, name string, req schema.GetObjectRequest) (*schema.Object, error) {
	var response getObjectResponse
	if err := c.DoWithContext(ctx,
		client.NewRequestEx(http.MethodHead, ""),
		&response,
		client.OptPath(name, req.Path),
	); err != nil {
		return nil, err
	}
	return response.Object, nil
}

// ReadObject downloads the content of an object using GET.
// The caller is responsible for closing the returned reader.
func (c *Client) ReadObject(ctx context.Context, name string, req schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error) {
	var response readObjectResponse
	if err := c.DoWithContext(ctx,
		client.NewRequest(),
		&response,
		client.OptPath(name, req.Path),
	); err != nil {
		return nil, nil, err
	}
	return response.Body, response.Object, nil
}

// StreamObject returns a truly streaming reader for the object body by making a
// direct HTTP GET, bypassing go-client's response-body lifecycle. The caller
// must close the returned ReadCloser. meta is nil when no object header is present.
func (c *Client) StreamObject(ctx context.Context, name string, req schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error) {
	// Build a properly encoded URL matching what OptPath(name, req.Path) produces.
	// url.JoinPath encodes each segment so spaces, '#', etc. are safe.
	rawURL, err := url.JoinPath(c.baseURL, name, req.Path)
	if err != nil {
		return nil, nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := c.httpClient().Do(httpReq)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("StreamObject: unexpected status %s", resp.Status)
	}
	var obj *schema.Object
	if metaJSON := resp.Header.Get(schema.ObjectMetaHeader); metaJSON != "" {
		var o schema.Object
		if err := json.Unmarshal([]byte(metaJSON), &o); err != nil {
			resp.Body.Close()
			return nil, nil, err
		}
		obj = &o
	}
	return resp.Body, obj, nil
}

// DeleteObject deletes a single object.
func (c *Client) DeleteObject(ctx context.Context, name string, req schema.DeleteObjectRequest) (*schema.Object, error) {
	var response schema.Object
	if err := c.DoWithContext(ctx,
		client.NewRequestEx(http.MethodDelete, "application/json"),
		&response,
		client.OptPath(name, req.Path),
	); err != nil {
		return nil, err
	}
	return &response, nil
}

// DeleteObjects deletes objects under a path prefix (recursive or non-recursive bulk delete).
func (c *Client) DeleteObjects(ctx context.Context, name string, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	query := make(url.Values)
	// Always send ?recursive so the server routes to the bulk-delete handler.
	// The value controls whether the deletion descends into sub-directories.
	query.Set("recursive", strconv.FormatBool(req.Recursive))
	var response schema.DeleteObjectsResponse
	if err := c.DoWithContext(ctx,
		client.NewRequestEx(http.MethodDelete, "application/json"),
		&response,
		client.OptPath(name, req.Path),
		client.OptQuery(query),
	); err != nil {
		return nil, err
	}
	return &response, nil
}
