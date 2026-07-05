package google

import (
	"context"
	"io"
	"net/url"

	// Packages

	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) Name() string {
	return c.url.Host
}

func (c *Client) URL() *url.URL {
	return c.url
}

////////////////////////////////////////////////////////////////////////////////
// STUBS

func (c *Client) CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error) {
	return nil, gofiler.ErrNotImplemented.With("CreateObject")
}

func (c *Client) GetObject(context.Context, schema.GetObjectRequest) (*schema.Object, error) {
	return nil, gofiler.ErrNotImplemented.With("GetObject")
}

func (c *Client) ReadObject(context.Context, schema.GetObjectRequest) (io.ReadCloser, *schema.Object, error) {
	return nil, nil, gofiler.ErrNotImplemented.With("ReadObject")
}

func (c *Client) ListObjects(context.Context, *schema.ObjectListIterator) error {
	return gofiler.ErrNotImplemented.With("ListObjects")
}

func (c *Client) DeleteObjects(context.Context, schema.DeleteObjectsRequest) error {
	return gofiler.ErrNotImplemented.With("DeleteObjects")
}
