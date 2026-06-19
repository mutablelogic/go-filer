package httpclient

import (
	"context"
	"io"

	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type namedReader interface {
	io.Reader
	Name() string
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// GetMetadata extracts metadata for a single file by POSTing it to the
// metadata endpoint as multipart/form-data.
func (c *Client) GetMetadata(ctx context.Context, r io.Reader) (*schema.ObjectMeta, error) {
	if r == nil {
		return nil, gofiler.ErrBadParameter.With("reader is required")
	}

	body := io.NopCloser(r)
	if rc, ok := r.(io.ReadCloser); ok {
		body = rc
	}

	var name string
	if fr, ok := r.(namedReader); ok {
		if n := fr.Name(); n != "" {
			name = n
		}
	}

	payload, err := client.NewStreamingMultipartRequest(&struct {
		File types.File `json:"file"`
	}{
		File: types.File{
			Path: name,
			Body: body,
		},
	}, types.ContentTypeJSON)
	if err != nil {
		return nil, err
	}

	var response schema.ObjectMeta
	if err := c.DoWithContext(ctx, payload, &response, client.OptPath("metadata")); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}
