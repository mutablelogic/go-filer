package task

import (
	"context"
	"fmt"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	rss "github.com/mutablelogic/go-filer/pkg/rss"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) FetchFeed(ctx context.Context, payload any) error {
	var rss rss.Feed

	// Decode the payload
	var url schema.Url
	if err := t.queue.UnmarshalPayload(&url, payload); err != nil {
		return err
	}

	// Refetch the url in case it has changed
	url2, err := t.feed.GetUrl(ctx, url.Id)
	if err != nil {
		return err
	}

	// Read the RSS feed
	if err := t.client.DoWithContext(ctx, nil, &rss, client.OptReqEndpoint(types.PtrString(url2.Url))); err != nil {
		return err
	}

	fmt.Println("UpdateFeed", rss)

	// Return sucess
	return nil
}
