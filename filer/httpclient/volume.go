package httpclient

import (
	"context"
	"errors"
	"net/http"
	"net/url"

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

// ReindexVolume requests reindexing of a volume. The response is a text/event-stream:
// "progress" events report the running total of objects considered, invoking callback
// if it is not nil, and a final "error" event reports any error encountered.
func (c *Client) ReindexVolume(ctx context.Context, name string, meta schema.ObjectListFilters, force bool, callback func(total uint64)) error {
	// Make the request payload
	req, err := client.NewJSONRequest(meta)
	if err != nil {
		return err
	}

	// Make the query
	query := url.Values{}
	if force {
		query.Set("force", "true")
	}

	// Perform request, decoding the text/event-stream response
	return c.DoWithContext(ctx, req, nil,
		client.OptPath("volume", name, "reindex"),
		client.OptQuery(query),
		client.OptTextStreamCallback(func(evt client.TextStreamEvent) error {
			switch evt.Event {
			case schema.EventProgress:
				var total uint64
				if err := evt.Json(&total); err != nil {
					return err
				}
				if callback != nil {
					callback(total)
				}
			case schema.EventError:
				var message string
				if err := evt.Json(&message); err != nil {
					return err
				}
				return errors.New(message)
			}
			return nil
		}),
	)
}
