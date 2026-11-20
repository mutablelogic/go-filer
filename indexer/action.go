package indexer

import (
	"context"
	"encoding/json"
	"slices"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// action is an action of the backend, whose object is indexed, once it's
// done, as an object which is written, or deleted, is
type action struct {
	gofiler.Action
	indexer *Indexer
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Actions returns the actions of the backend, which queue the index of the
// object each creates, changes, or deletes, once it's done, so the index
// doesn't wait for the backend's watch, or its next scan
func (self *Indexer) Actions() []gofiler.Action {
	actions := self.Filer.Actions()
	if len(actions) == 0 {
		return nil
	}
	result := make([]gofiler.Action, 0, len(actions))
	for _, a := range actions {
		result = append(result, action{Action: a, indexer: self})
	}
	return result
}

// Run does the backend's action, and queues the index of its object: what
// the index has at its path, a file's, or a folder's, or both, and its own
// key, which updates its folder, so one which is created is added, one which
// is changed is updated, and one which is deleted is removed
func (a action) Run(ctx context.Context, input json.RawMessage) (*schema.Object, error) {
	object, err := a.Action.Run(ctx, input)
	if err != nil || object == nil {
		return object, err
	}
	keys := a.indexer.keysAt(ctx, object.Path)
	if key := store.Key(object); !slices.Contains(keys, key) {
		keys = append(keys, key)
	}
	if err := a.indexer.queue.index(ctx, keys...); err != nil {
		a.indexer.failed(ctx, err)
	}
	return object, nil
}
