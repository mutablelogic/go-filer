package task

import (
	"context"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/feed/schema"
	ref "github.com/mutablelogic/go-server/pkg/ref"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) FetchItem(ctx context.Context, payload any) error {
	var key schema.ItemId
	if err := t.queue.UnmarshalPayload(&key, payload); err != nil {
		return err
	}

	// Get the feed item
	item, err := t.feed.GetItem(ctx, key.Feed, key.Guid)
	if err != nil {
		return err
	} else if item.Block {
		ref.Log(ctx).With("item", item).Debug(ctx, "Item is blocked, not updating")
		return nil
	}

	ref.Log(ctx).With("item", item).Print(ctx, "Fetched item")

	// Return sucess
	return nil
}
