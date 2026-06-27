package httpclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type getObjectResponse struct {
	Object *schema.Object
}

var _ client.Unmarshaler = (*getObjectResponse)(nil)

func (r *getObjectResponse) Unmarshal(header http.Header, _ io.Reader) error {
	data := header.Get(schema.ContentObjectHeader)
	if data == "" {
		return fmt.Errorf("missing %s header", schema.ContentObjectHeader)
	}
	var obj schema.Object
	if err := json.Unmarshal([]byte(data), &obj); err != nil {
		return err
	}
	r.Object = &obj
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) GetObject(ctx context.Context, volume, path string) (*schema.Object, error) {
	var response getObjectResponse
	if err := c.DoWithContext(ctx,
		client.NewRequestEx(http.MethodHead, ""),
		&response,
		client.OptPath("object", volume, path),
	); err != nil {
		return nil, err
	}
	return response.Object, nil
}

func (c *Client) ListObjects(ctx context.Context, req schema.ObjectListRequest) (*schema.ObjectList, error) {
	var response schema.ObjectList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("object"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
