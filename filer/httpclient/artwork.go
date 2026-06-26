package httpclient

import (
	"context"
	"io"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

type NamedReader interface {
	io.Reader
	Name() string
}

func (c *Client) CreateArtwork(ctx context.Context, r io.ReadCloser) (*schema.Artwork, error) {
	// Set path
	var path string
	if nr, ok := r.(NamedReader); ok {
		path = nr.Name()
	}

	// Create a file upload request
	request := schema.ArtworkUploadRequest{
		Data: types.File{
			Path: path,
			Body: r,
		},
	}

	// Upload a file
	var response schema.Artwork
	if payload, err := client.NewStreamingMultipartRequest(request, types.ContentTypeJSON); err != nil {
		return nil, err
	} else if err := c.Do(payload, &response, client.OptPath("artwork")); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
