package httpclient

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListObjects returns a list of objects at the given backend name and optional path prefix.
func (c *Client) ListObjects(ctx context.Context, name string, req schema.ListObjectsRequest) (*schema.ListObjectsResponse, error) {
	query := make(url.Values)
	// Path is a filter prefix, not a URL path segment — always navigate to /{name}
	// and pass path as a query parameter so the server routes to the list handler.
	if req.Path != "" {
		query.Set("path", req.Path)
	}
	if req.Recursive {
		query.Set("recursive", "true")
	}
	if req.Offset > 0 {
		query.Set("offset", strconv.Itoa(req.Offset))
	}
	if req.Limit > 0 {
		query.Set("limit", strconv.Itoa(req.Limit))
	}
	var response schema.ListObjectsResponse
	if err := c.DoWithContext(ctx, client.NewRequest(), &response,
		client.OptPath(name),
		client.OptQuery(query),
	); err != nil {
		return nil, err
	}
	return &response, nil
}

// CreateObject uploads content using PUT, forwarding ContentType, ModTime and Meta as request headers.
func (c *Client) CreateObject(ctx context.Context, name string, req schema.CreateObjectRequest) (*schema.Object, error) {
	payload := &putPayload{body: req.Body, contentType: req.ContentType}

	// Build per-request header opts from the request metadata
	opts := []client.RequestOpt{client.OptPath(name, req.Path)}
	if !req.ModTime.IsZero() {
		opts = append(opts, client.OptReqHeader("Last-Modified", req.ModTime.Format(http.TimeFormat)))
	}
	// If-None-Match: * triggers the server's IfNotExists check (RFC 7232 §3.2).
	if req.IfNotExists {
		opts = append(opts, client.OptReqHeader("If-None-Match", "*"))
	}
	for k, v := range req.Meta {
		opts = append(opts, client.OptReqHeader(http.CanonicalHeaderKey(schema.ObjectMetaKeyPrefix+k), v))
	}

	var response schema.Object
	if err := c.DoWithContext(ctx, payload, &response, opts...); err != nil {
		return nil, err
	}
	return &response, nil
}

// GetObject retrieves metadata only for an object using HEAD (no body download).
func (c *Client) GetObject(ctx context.Context, name string, req schema.GetObjectRequest) (*schema.Object, error) {
	var response getObjectResponse
	if err := c.DoWithContext(ctx,
		client.NewRequestEx(http.MethodHead, ""),
		&response,
		client.OptPath(name, req.Path),
	); err != nil {
		return nil, err
	}
	if response.Object == nil {
		return nil, fmt.Errorf("GetObject: missing %s header in response", schema.ObjectMetaHeader)
	}
	return response.Object, nil
}

// ReadObject downloads the content of an object using GET, calling fn with each
// chunk of data as it arrives from the server. fn may be nil when only metadata
// is needed. The slice passed to fn is reused across calls; copy it if retained.
// Returns the object metadata; the returned *Object is always non-nil on success.
func (c *Client) ReadObject(ctx context.Context, name string, req schema.ReadObjectRequest, fn func([]byte) error) (*schema.Object, error) {
	u := &readObjectUnmarshaler{fn: fn}
	if err := c.DoWithContext(ctx,
		client.NewRequest(),
		u,
		client.OptPath(name, req.Path),
	); err != nil {
		return nil, err
	}
	if u.obj == nil {
		return nil, fmt.Errorf("ReadObject: missing %s header in response", schema.ObjectMetaHeader)
	}
	return u.obj, nil
}

// DeleteObject deletes a single object.
func (c *Client) DeleteObject(ctx context.Context, name string, req schema.DeleteObjectRequest) (*schema.Object, error) {
	var response schema.Object
	if err := c.DoWithContext(ctx,
		client.NewRequestEx(http.MethodDelete, "application/json"),
		&response,
		client.OptPath(name, req.Path),
	); err != nil {
		return nil, err
	}
	return &response, nil
}

// DeleteObjects deletes objects under a path prefix (recursive or non-recursive bulk delete).
func (c *Client) DeleteObjects(ctx context.Context, name string, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	var response schema.DeleteObjectsResponse

	// Always send ?recursive so the server routes to the bulk-delete handler.
	// The value controls whether the deletion descends into sub-directories.
	query := make(url.Values)
	query.Set("recursive", strconv.FormatBool(req.Recursive))

	// Use DELETE with an empty body since the request parameters are fully conveyed
	if err := c.DoWithContext(ctx,
		client.NewRequestEx(http.MethodDelete, "application/json"),
		&response,
		client.OptPath(name, req.Path),
		client.OptQuery(query),
	); err != nil {
		return nil, err
	}

	// Return the response, which includes the list of deleted objects and any errors.
	return &response, nil
}
