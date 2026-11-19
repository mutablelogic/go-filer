package httpclient

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListVolumes returns the volumes which are registered, with the URLs of
// their backends, and the schemes of the URLs which volumes can be created
// from
func (c *Client) ListVolumes(ctx context.Context) (*schema.VolumeList, error) {
	var response schema.VolumeList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("volume")); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// GetVolume returns a volume by its name, with the URL of its backend
func (c *Client) GetVolume(ctx context.Context, name string) (*schema.Volume, error) {
	var response schema.Volume
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("volume", name)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// UpdateVolume changes the options of a volume by its name, and whether it's
// enabled, so it's mounted, when enabled isn't nil, and returns it, with the
// URL of its backend. An option which is null is removed, so it's the
// default, and those which aren't in the patch are kept.
func (c *Client) UpdateVolume(ctx context.Context, name string, patch schema.VolumePatch, enabled *bool) (*schema.Volume, error) {
	if patch == nil {
		patch = schema.VolumePatch{}
	}
	req, err := client.NewJSONRequestEx(http.MethodPatch, patch, types.ContentTypeAny)
	if err != nil {
		return nil, err
	}
	opts := []client.RequestOpt{client.OptPath("volume", name)}
	if enabled != nil {
		opts = append(opts, client.OptQuery(url.Values{"enabled": {strconv.FormatBool(*enabled)}}))
	}

	// Perform request
	var response schema.Volume
	if err := c.DoWithContext(ctx, req, &response, opts...); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// DeleteVolume deletes a volume by its name, and returns it, with the URL of
// its backend, which it can be created again from. The volume's files
// aren't deleted.
func (c *Client) DeleteVolume(ctx context.Context, name string) (*schema.Volume, error) {
	var response schema.Volume
	if err := c.DoWithContext(ctx, client.MethodDelete, &response, client.OptPath("volume", name)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// CreateVolume creates a volume with a name, from the URL of its backend,
// which is enabled, unless its Enabled is false, and returns it, with the
// URL of its backend, which has each of its options, defaults too
func (c *Client) CreateVolume(ctx context.Context, volume schema.VolumeCreateRequest) (*schema.Volume, error) {
	req, err := client.NewJSONRequest(volume)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Volume
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("volume")); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}
