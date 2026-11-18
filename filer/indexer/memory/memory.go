// Package memory is an index store in memory, for tests, and volumes which
// are indexed again each time a process starts
package memory

import (
	"context"
	"slices"
	"strings"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/filer/indexer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Store is an index in memory
type Store struct {
	mu      sync.RWMutex
	objects map[string]map[string]*schema.Object // By volume, and path
	states  map[string]indexer.State             // By volume
}

var _ indexer.Store = (*Store)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns an empty index in memory
func New() *Store {
	return &Store{objects: make(map[string]map[string]*schema.Object), states: make(map[string]indexer.State)}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

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
		if p == path || indexer.Beneath(path, p) {
			delete(self.objects[volume], p)
		}
	}
	return nil
}

// List returns the objects of a folder, or every object beneath it, in the
// order of their keys, after the key it starts after, up to the limit
func (self *Store) List(ctx context.Context, volume string, req indexer.ListRequest) ([]*schema.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	self.mu.RLock()
	var result []*schema.Object
	for p, object := range self.objects[volume] {
		switch {
		case req.Recursive && !indexer.Beneath(req.Parent, p):
			continue
		case !req.Recursive && (p == "/" || indexer.Parent(p) != req.Parent):
			continue
		case req.After != "" && strings.Compare(indexer.Key(object), req.After) <= 0:
			continue
		}
		result = append(result, clone(object))
	}
	self.mu.RUnlock()
	slices.SortFunc(result, func(a, b *schema.Object) int {
		return strings.Compare(indexer.Key(a), indexer.Key(b))
	})
	if req.Limit > 0 && len(result) > req.Limit {
		result = result[:req.Limit]
	}
	return result, nil
}

// State returns the state of the volume's index
func (self *Store) State(ctx context.Context, volume string) (indexer.State, error) {
	if err := ctx.Err(); err != nil {
		return indexer.State{}, err
	}
	self.mu.RLock()
	defer self.mu.RUnlock()
	return self.states[volume], nil
}

// SetState stores the state of the volume's index
func (self *Store) SetState(ctx context.Context, volume string, state indexer.State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	self.mu.Lock()
	defer self.mu.Unlock()
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
