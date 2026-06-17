package httpclient

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) CreateVolume(ctx context.Context, volume schema.VolumeCreate) (*schema.Volume, error) {
	req, err := client.NewJSONRequest(volume)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Volume
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("volume")); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
