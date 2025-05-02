package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Enumerate the objects in a bucket, optionally with a prefix
func (c *Client) ListObjects(ctx context.Context, bucket string, opts ...Opt) (*schema.ObjectList, error) {
	// Make request
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.ObjectList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("object", bucket), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

// Get object metadata
func (c *Client) GetObject(ctx context.Context, bucket, key string) (*schema.Object, error) {
	// Make request
	req := client.NewRequestEx(http.MethodHead, "")

	// Perform request
	var response getobjectresponse
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("object", bucket, key)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response.Object, nil
}

// Delete an object
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	// Make request
	req := client.NewRequestEx(http.MethodDelete, "")

	// Perform request
	return c.DoWithContext(ctx, req, nil, client.OptPath("object", bucket, key))
}

// Write (download) the contents of an object
func (c *Client) WriteObject(ctx context.Context, w io.Writer, bucket, key string) error {
	// Make request
	req := client.NewRequest()

	// Perform request
	return c.DoWithContext(ctx, req, w, client.OptPath("object", bucket, key))
}

// Create objects from a file or directory
func (c *Client) CreateObjects(ctx context.Context, bucket string, path []string, opts ...Opt) (*schema.ObjectList, error) {
	// Make the uploader request
	uploader := NewUploader(opts...)

	// Add paths
	for _, path := range path {
		if err := uploader.Add("file", path); err != nil {
			return nil, err
		}
	}

	// Indicate ready to upload
	if err := uploader.Close(); err != nil {
		return nil, err
	}

	// Perform request - with no timeout
	var response schema.ObjectList
	if err := c.DoWithContext(ctx, uploader, &response, client.OptPath("object", bucket), client.OptNoTimeout()); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// Response object to get the X-Object-Meta header
type getobjectresponse struct {
	schema.Object
}

func (r *getobjectresponse) Unmarshal(header http.Header, _ io.Reader) error {
	// Check for X-Object-Meta header
	meta := header.Get("X-Object-Meta")
	if meta == "" {
		return httpresponse.ErrInternalError.With("missing meta header")
	}
	return json.Unmarshal([]byte(meta), &r.Object)
}
