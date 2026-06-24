package httpclient

import (
	"context"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListVolumes(ctx context.Context, req schema.VolumeListRequest) (*schema.VolumeList, error) {
	var response schema.VolumeList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("volume"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

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

func (c *Client) GetVolume(ctx context.Context, name string) (*schema.Volume, error) {
	var response schema.Volume
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("volume", name)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

func (c *Client) UpdateVolume(ctx context.Context, name string, meta schema.VolumeMeta) (*schema.Volume, error) {
	req, err := client.NewJSONRequestEx(http.MethodPatch, meta, types.ContentTypeAny)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Volume
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("volume", name)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

func (c *Client) DeleteVolume(ctx context.Context, name string) (*schema.Volume, error) {
	var response schema.Volume
	if err := c.DoWithContext(ctx, client.MethodDelete, &response, client.OptPath("volume", name)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}
