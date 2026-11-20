package httpclient

import (
	"context"
	"encoding/json"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListActions returns the actions of a volume, in order of their names
func (c *Client) ListActions(ctx context.Context, volume string) (*schema.ActionList, error) {
	var response schema.ActionList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("volume", volume, "action")); err != nil {
		return nil, err
	}
	return types.Ptr(response), nil
}

// CallAction calls an action of a volume, of its name, with its input, which
// is JSON of its schema, or {} when it's empty, and returns the object it
// created or changed
func (c *Client) CallAction(ctx context.Context, volume, action string, input json.RawMessage) (*schema.Object, error) {
	if len(input) == 0 {
		input = json.RawMessage("{}")
	}
	req, err := client.NewJSONRequestEx(http.MethodPost, input, types.ContentTypeAny)
	if err != nil {
		return nil, err
	}
	var response schema.Object
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("volume", volume, "action", action)); err != nil {
		return nil, err
	}
	return types.Ptr(response), nil
}
