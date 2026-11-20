package registry

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListActions returns the actions of a volume's backend, in order of their
// names, which is empty for a backend which has none, such as a
// filesystem. A volume which isn't mounted is as GetObject's, and one which
// is offline, as its watch failed, is ErrServiceUnavailable, as actions are
// done of its backend, whose changes are of its watch.
func (self *Registry) ListActions(ctx context.Context, volume string) (*schema.ActionList, error) {
	result := &schema.ActionList{Name: volume, Body: []schema.Action{}}
	if err := self.withOnline(ctx, volume, func(ctx context.Context, b gofiler.Filer) error {
		for _, action := range b.Actions() {
			result.Body = append(result.Body, gofiler.ActionOf(action))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	slices.SortFunc(result.Body, func(a, b schema.Action) int { return strings.Compare(a.Name, b.Name) })
	return result, nil
}

// CallAction runs an action of a volume's backend, of its name, with its
// input, which is JSON of its schema, and returns the object it created or
// changed, with the volume's name. An action which the backend doesn't have
// is ErrNotFound, and a volume which isn't mounted, or is offline, is as
// ListActions'.
func (self *Registry) CallAction(ctx context.Context, volume, name string, input json.RawMessage) (*schema.Object, error) {
	var object *schema.Object
	if err := self.withOnline(ctx, volume, func(ctx context.Context, b gofiler.Filer) (err error) {
		for _, action := range b.Actions() {
			if action.Name() == name {
				object, err = action.Run(ctx, input)
				return err
			}
		}
		return gofiler.ErrNotFound.Withf("volume %q has no action %q", volume, name)
	}); err != nil {
		return nil, err
	} else if object == nil {
		return nil, gofiler.ErrInternalServerError.Withf("action %q of volume %q returned no object", name, volume)
	}
	object.Name = volume
	return object, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// withOnline calls the function with the backend of a volume, as With does,
// unless it's offline, as its watch failed, which is ErrServiceUnavailable,
// with why
func (self *Registry) withOnline(ctx context.Context, volume string, fn func(context.Context, gofiler.Filer) error) error {
	return self.With(ctx, volume, func(ctx context.Context, b gofiler.Filer) error {
		self.RLock()
		var offline error
		if e, exists := self.backend[volume]; exists {
			offline = e.offline
		}
		self.RUnlock()
		if offline != nil {
			return gofiler.ErrServiceUnavailable.Withf("volume %q is offline: %v", volume, offline)
		}
		return fn(ctx, b)
	})
}
