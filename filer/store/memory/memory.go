// Package memory is a store of volumes, and their indexes, in memory, for
// tests, and volumes which are indexed again each time a process starts
package memory

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	store "github.com/mutablelogic/go-filer/filer/store"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Store is a store of volumes, and their indexes, in memory
type Store struct {
	mu      sync.RWMutex
	opts    store.Opts
	volumes map[string]schema.Volume             // By name
	objects map[string]map[string]*schema.Object // By volume, and path
	states  map[string]schema.IndexState         // By volume
}

var _ store.Store = (*Store)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns an empty index in memory
func New(opts ...store.Opt) (*Store, error) {
	o, err := store.Apply(opts...)
	if err != nil {
		return nil, err
	}
	return &Store{opts: o, volumes: make(map[string]schema.Volume), objects: make(map[string]map[string]*schema.Object), states: make(map[string]schema.IndexState)}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// PutVolume stores a volume, or replaces its URL, and whether it's
// enabled, keeping its index
func (self *Store) PutVolume(ctx context.Context, meta schema.VolumeMeta) (*schema.Volume, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	} else if err := store.ValidateVolume(meta); err != nil {
		return nil, err
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	now := time.Now().UTC()
	volume := schema.Volume{VolumeMeta: meta}
	if existing, exists := self.volumes[meta.Name]; !exists {
		volume.Created = now
		defer self.opts.Publish(changes.ChangeCreate, &volume)
	} else if existing.VolumeMeta == meta {
		volume = existing
	} else {
		volume.Created, volume.Modified = existing.Created, now
		defer self.opts.Publish(changes.ChangeUpdate, &volume)
	}
	self.volumes[meta.Name] = volume
	return &volume, nil
}

// GetVolume returns a volume, or ErrNotFound
func (self *Store) GetVolume(ctx context.Context, name string) (*schema.Volume, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	self.mu.RLock()
	defer self.mu.RUnlock()
	if volume, exists := self.volumes[name]; exists {
		return &volume, nil
	}
	return nil, gofiler.ErrNotFound.Withf("volume %q", name)
}

// ListVolumes returns the volumes, in the order of their names
func (self *Store) ListVolumes(ctx context.Context) ([]*schema.Volume, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	self.mu.RLock()
	result := make([]*schema.Volume, 0, len(self.volumes))
	for _, volume := range self.volumes {
		result = append(result, &volume)
	}
	self.mu.RUnlock()
	slices.SortFunc(result, func(a, b *schema.Volume) int {
		return strings.Compare(a.Name, b.Name)
	})
	return result, nil
}

// DeleteVolume removes a volume, with its index, or returns ErrNotFound
func (self *Store) DeleteVolume(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	volume, exists := self.volumes[name]
	if !exists {
		return gofiler.ErrNotFound.Withf("volume %q", name)
	}
	defer self.opts.Publish(changes.ChangeDelete, &volume)
	delete(self.volumes, name)
	delete(self.objects, name)
	delete(self.states, name)
	return nil
}

// Get returns the object of the volume at the path, or ErrNotFound
func (self *Store) Get(ctx context.Context, volume, path string) (*schema.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	self.mu.RLock()
	defer self.mu.RUnlock()
	if object, exists := self.objects[volume][path]; exists {
		return clone(object), nil
	}
	return nil, gofiler.ErrNotFound.Withf("%q isn't indexed in volume %q", path, volume)
}

// Put stores the objects of the volume, replacing those at their paths
func (self *Store) Put(ctx context.Context, volume string, objects ...*schema.Object) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	if _, exists := self.volumes[volume]; !exists {
		return gofiler.ErrNotFound.Withf("volume %q", volume)
	}
	if self.objects[volume] == nil {
		self.objects[volume] = make(map[string]*schema.Object)
	}
	for _, object := range objects {
		self.objects[volume][object.Path] = clone(object)
	}
	return nil
}

// Delete removes the object at the path, and those beneath it
func (self *Store) Delete(ctx context.Context, volume, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	for p := range self.objects[volume] {
		if p == path || store.Beneath(path, p) {
			delete(self.objects[volume], p)
		}
	}
	return nil
}

// List returns the objects of a folder, or every object beneath it, in the
// order of their keys, after the key it starts after, up to the limit
func (self *Store) List(ctx context.Context, volume string, req store.ListRequest) ([]*schema.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	self.mu.RLock()
	var result []*schema.Object
	for p, object := range self.objects[volume] {
		switch {
		case req.Recursive && !store.Beneath(req.Parent, p):
			continue
		case !req.Recursive && (p == "/" || store.Parent(p) != req.Parent):
			continue
		case req.After != "" && strings.Compare(store.Key(object), req.After) <= 0:
			continue
		}
		result = append(result, clone(object))
	}
	self.mu.RUnlock()
	slices.SortFunc(result, func(a, b *schema.Object) int {
		return strings.Compare(store.Key(a), store.Key(b))
	})
	if req.Limit > 0 && len(result) > req.Limit {
		result = result[:req.Limit]
	}
	return result, nil
}

// State returns the state of the volume's index
func (self *Store) State(ctx context.Context, volume string) (schema.IndexState, error) {
	if err := ctx.Err(); err != nil {
		return schema.IndexState{}, err
	}
	self.mu.RLock()
	defer self.mu.RUnlock()
	return self.states[volume], nil
}

// SetState stores the state of the volume's index
func (self *Store) SetState(ctx context.Context, volume string, state schema.IndexState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	if _, exists := self.volumes[volume]; !exists {
		return gofiler.ErrNotFound.Withf("volume %q", volume)
	}
	self.states[volume] = state
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// clone returns a copy of an object, with its metadata's values, so what's
// stored isn't changed by its caller
func clone(object *schema.Object) *schema.Object {
	result := *object
	result.Meta = slices.Clone(object.Meta)
	for i := range result.Meta {
		result.Meta[i].Value = slices.Clone(result.Meta[i].Value)
	}
	if object.Size != nil {
		size := *object.Size
		result.Size = &size
	}
	if object.ETag != nil {
		etag := *object.ETag
		result.ETag = &etag
	}
	return &result
}
