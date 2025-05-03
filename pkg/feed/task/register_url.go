package task

import (
	"context"

	// Packages

	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	"github.com/mutablelogic/go-filer/pkg/rss"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) RegisterUrl(ctx context.Context, url *schema.Url) error {
	var rss rss.Feed

	// Read the RSS feed
	if err := t.client.DoWithContext(ctx, nil, &rss, client.OptReqEndpoint(types.PtrString(url.Url))); err != nil {
		return err
	}

	// Insert the feed
	_, err := t.feed.CreateFeed(ctx, url.Id, rss)
	if err != nil {
		return err
	}

	// Return sucess
	return nil
}
