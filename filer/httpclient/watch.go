package httpclient

import (
	"context"
	"fmt"
	"net/url"

	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// eventTypes are the changes to objects, by the names of their events
var eventTypes = map[string]schema.EventType{
	schema.EventTypeCreated.String(): schema.EventTypeCreated,
	schema.EventTypeUpdated.String(): schema.EventTypeUpdated,
	schema.EventTypeDeleted.String(): schema.EventTypeDeleted,
	schema.EventTypeRescan.String():  schema.EventTypeRescan,
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WatchObjects calls the function with each change to the objects of the
// volumes, or every volume when there are none, until the context is
// cancelled, when it returns nil, or the function returns an error, which is
// returned. Returns gofiler.ErrServiceUnavailable when the server is stopping.
func (c *Client) WatchObjects(ctx context.Context, names []string, fn func(schema.Event) error) error {
	if fn == nil {
		return gofiler.ErrBadParameter.With("missing function")
	}

	// Read the changes, and ignore the pings. The function's error is kept,
	// as it's returned even when the context is cancelled as well.
	var fnErr, shutdown error
	callback := func(event client.TextStreamEvent) error {
		if event.Event == "shutdown" {
			shutdown = gofiler.ErrServiceUnavailable.With("server is stopping")
			return nil
		}
		eventType, ok := eventTypes[event.Event]
		if !ok {
			return nil
		}
		var key schema.ObjectKey
		if err := event.Json(&key); err != nil {
			return fmt.Errorf("malformed %s event: %w", event.Event, err)
		}
		fnErr = fn(schema.Event{EventType: eventType, ObjectKey: key})
		return fnErr
	}
	err := c.DoWithContext(ctx, client.MethodGet, nil,
		client.OptPath("watch"),
		client.OptQuery(url.Values{"name": names}),
		client.OptReqHeader("Accept", types.ContentTypeTextStream),
		client.OptTextStreamCallback(callback),
		client.OptNoTimeout(),
	)
	switch {
	case fnErr != nil:
		return fnErr
	case ctx.Err() != nil:
		return nil
	case shutdown != nil:
		return shutdown
	default:
		return err
	}
}
