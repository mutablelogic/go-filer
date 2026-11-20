package registry

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListActions returns the actions of a volume's backend, or, when the
// volume is empty, of every volume which is mounted, and online, in order of
// their volumes, and their names, which is empty for a backend which has
// none, such as a filesystem. A volume which isn't mounted is as GetObject's,
// and one which is offline, as its watch failed, is ErrServiceUnavailable,
// as actions are done of its backend, whose changes are of its watch.
func (self *Registry) ListActions(ctx context.Context, volume string) (*schema.ActionList, error) {
	result := &schema.ActionList{Body: []schema.Action{}}
	if volume != "" {
		actions, err := self.actionsOf(ctx, volume)
		if err != nil {
			return nil, err
		}
		result.Body = append(result.Body, actions...)
		return result, nil
	}

	// Every volume which is mounted, apart from those which are offline, or
	// are unmounted meanwhile
	for _, volume := range self.Keys() {
		actions, err := self.actionsOf(ctx, volume)
		if errors.Is(err, gofiler.ErrServiceUnavailable) || errors.Is(err, gofiler.ErrNotFound) {
			continue
		} else if err != nil {
			return nil, err
		}
		result.Body = append(result.Body, actions...)
	}
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

// actionsOf returns the actions of a volume, in order of their names, with
// the volume's name
func (self *Registry) actionsOf(ctx context.Context, volume string) ([]schema.Action, error) {
	var result []schema.Action
	if err := self.withOnline(ctx, volume, func(ctx context.Context, b gofiler.Filer) error {
		for _, action := range b.Actions() {
			listed := gofiler.ActionOf(action)
			listed.Volume = volume
			result = append(result, listed)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	slices.SortFunc(result, func(a, b schema.Action) int { return strings.Compare(a.Name, b.Name) })
	return result, nil
}

// withOnline calls the function with the backend of a volume, as With does,
// unless it's offline, as its watch failed, which is ErrServiceUnavailable,
// with why, of the entry which is used, rather than one which replaces it
// meanwhile, such as of a remount
func (self *Registry) withOnline(ctx context.Context, volume string, fn func(context.Context, gofiler.Filer) error) error {
	e, err := self.use(ctx, volume)
	if err != nil {
		return err
	}
	defer e.wg.Done()
	return self.runOnline(ctx, volume, e, fn)
}

// runOnline calls the function with the backend of the entry, which is
// used, unless the entry is offline, with a context which is cancelled when
// the volume is removed, as With does
func (self *Registry) runOnline(ctx context.Context, volume string, e *entry, fn func(context.Context, gofiler.Filer) error) error {
	self.RLock()
	offline := e.offline
	self.RUnlock()
	if offline != nil {
		return gofiler.ErrServiceUnavailable.Withf("volume %q is offline: %v", volume, offline)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(e.ctx, cancel)
	defer stop()
	return fn(ctx, e.Filer)
}
