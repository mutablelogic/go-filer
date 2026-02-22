package httpclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// putPayload implements client.Payload for PUT requests with a body
type putPayload struct {
	body   io.Reader
	length *int64
}

// getObjectResponse holds the response from a GetObject request
type getObjectResponse struct {
	Body   io.ReadCloser
	Object *schema.Object
}

// headObjectResponse holds the response from a GetObjectMeta request
type headObjectResponse struct {
	Object *schema.Object
}

// Ensure putPayload implements client.Payload
var _ client.Payload = (*putPayload)(nil)

// Ensure getObjectResponse implements client.Unmarshaler
var _ client.Unmarshaler = (*getObjectResponse)(nil)

// Ensure headObjectResponse implements client.Unmarshaler
var _ client.Unmarshaler = (*headObjectResponse)(nil)

func (p *putPayload) Method() string {
	return http.MethodPut
}

func (p *putPayload) Accept() string {
	return "application/json"
}

func (p *putPayload) Type() string {
	return "application/octet-stream"
}

func (p *putPayload) Read(b []byte) (int, error) {
	return p.body.Read(b)
}

func (p *putPayload) ContentLength() int64 {
	if p.length != nil {
		return *p.length
	}
	return -1 // unknown length
}

func (r *getObjectResponse) Unmarshal(header http.Header, reader io.Reader) error {
	// Parse object metadata from X-Object-Meta header
	if metaJSON := header.Get(schema.ObjectMetaHeader); metaJSON != "" {
		var obj schema.Object
		if err := json.Unmarshal([]byte(metaJSON), &obj); err != nil {
			return err
		}
		r.Object = &obj
	}

	// Store the reader as-is (caller will close it)
	if rc, ok := reader.(io.ReadCloser); ok {
		r.Body = rc
	} else {
		r.Body = io.NopCloser(reader)
	}

	return nil
}

func (r *headObjectResponse) Unmarshal(header http.Header, reader io.Reader) error {
	// Parse object metadata from X-Object-Meta header
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

// ListObjects returns a list of objects at the given URL.
// The URL should be in the format scheme://host/path (e.g., file://media/podcasts).
func (c *Client) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (*schema.ListObjectsResponse, error) {
	// Parse the URL to extract scheme, host, and path
	u, err := url.Parse(req.URL)
	if err != nil {
		return nil, err
	}

	// Build path: /{scheme}/{host}{path}
	path := u.Scheme + "/" + u.Host + u.Path

	// Build query params
	query := make(url.Values)
	if req.Recursive {
		query.Set("recursive", "true")
	}

	// Create request
	request := client.NewRequest()

	// Perform request
	var response schema.ListObjectsResponse
	if err := c.DoWithContext(ctx, request, &response, client.OptPath(path), client.OptQuery(query)); err != nil {
		return nil, err
	}

	// Return the response
	return &response, nil
}

// CreateObject uploads a file to the given URL using PUT.
// The URL should be in the format scheme://host/path (e.g., file://media/podcasts/file.txt).
// The body is the file content, and size is optional (nil if unknown).
func (c *Client) CreateObject(ctx context.Context, urlStr string, body io.Reader, size *int64) (*schema.Object, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	// Build path: /{scheme}/{host}{path}
	path := u.Scheme + "/" + u.Host + u.Path

	// Create PUT payload
	payload := &putPayload{
		body:   body,
		length: size,
	}

	// Perform request
	var response schema.Object
	if err := c.DoWithContext(ctx, payload, &response, client.OptPath(path)); err != nil {
		return nil, err
	}

	return &response, nil
}

// GetObject downloads a file from the given URL using GET.
// The URL should be in the format scheme://host/path (e.g., file://media/podcasts/file.txt).
// Returns an io.ReadCloser for the file content and the object metadata.
// The caller is responsible for closing the reader.
func (c *Client) GetObject(ctx context.Context, urlStr string) (io.ReadCloser, *schema.Object, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, nil, err
	}

	// Build path: /{scheme}/{host}{path}
	path := u.Scheme + "/" + u.Host + u.Path

	// Create GET request
	request := client.NewRequest()

	// Create response object that implements Unmarshaler
	var response getObjectResponse

	// Perform request
	if err := c.DoWithContext(ctx, request, &response, client.OptPath(path)); err != nil {
		return nil, nil, err
	}

	// Return success
	return response.Body, response.Object, nil
}

// GetObjectMeta retrieves only the metadata for a file at the given URL using HEAD.
// The URL should be in the format scheme://host/path (e.g., file://media/podcasts/file.txt).
// Returns the object metadata without downloading the file content.
func (c *Client) GetObjectMeta(ctx context.Context, urlStr string) (*schema.Object, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	// Build path: /{scheme}/{host}{path}
	path := u.Scheme + "/" + u.Host + u.Path

	// Create HEAD request
	request := client.NewRequestEx(http.MethodHead, "")

	// Create response object that implements Unmarshaler
	var response headObjectResponse
	if err := c.DoWithContext(ctx, request, &response, client.OptPath(path)); err != nil {
		return nil, err
	}

	// Return the object metadata
	return response.Object, nil
}
