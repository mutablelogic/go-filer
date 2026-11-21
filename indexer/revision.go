package indexer

import (
	"context"
	"sync/atomic"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// revised is the store of an indexer, whose revision is incremented once
// it's written, of the index, its state, or its queue, so what's read of
// them needn't be read again until it changes
type revised struct {
	store.Store
	revision atomic.Uint64
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Revision returns the revision of the indexer's index, its state, and its
// queue, of this process, which changes once they're written, so their
// state needn't be read again while it's the same. It doesn't change when
// they're written of another process, such as another server of a store.
func (self *Indexer) Revision() uint64 {
	return self.store.(*revised).revision.Load()
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// revise increments the revision, once the store is written
func (s *revised) revise(err error) error {
	s.revision.Add(1)
	return err
}

func (s *revised) SetState(ctx context.Context, volume string, state schema.IndexState) error {
	return s.revise(s.Store.SetState(ctx, volume, state))
}

func (s *revised) PutObject(ctx context.Context, volume string, objects ...*schema.Object) error {
	return s.revise(s.Store.PutObject(ctx, volume, objects...))
}

func (s *revised) DeleteObject(ctx context.Context, volume, key string) error {
	return s.revise(s.Store.DeleteObject(ctx, volume, key))
}

func (s *revised) SetRevisions(ctx context.Context, volume string, revisions map[string]string) error {
	return s.revise(s.Store.SetRevisions(ctx, volume, revisions))
}

func (s *revised) SetMetadata(ctx context.Context, volume, key, source string, of *schema.ObjectAttr, found schema.TaskMeta) error {
	return s.revise(s.Store.SetMetadata(ctx, volume, key, source, of, found))
}

func (s *revised) DeleteMetadata(ctx context.Context, volume, source string) error {
	return s.revise(s.Store.DeleteMetadata(ctx, volume, source))
}

func (s *revised) Push(ctx context.Context, volume string, work ...store.Work) error {
	return s.revise(s.Store.Push(ctx, volume, work...))
}

func (s *revised) Done(ctx context.Context, volume string, work ...store.Work) error {
	return s.revise(s.Store.Done(ctx, volume, work...))
}

func (s *revised) PushStale(ctx context.Context, volume, folder string, kind store.Kind) error {
	return s.revise(s.Store.PushStale(ctx, volume, folder, kind))
}

func (s *revised) Failed(ctx context.Context, volume string, work store.Work, err error, retry time.Duration) error {
	return s.revise(s.Store.Failed(ctx, volume, work, err, retry))
}

func (s *revised) Sweep(ctx context.Context, volume, folder string) error {
	return s.revise(s.Store.Sweep(ctx, volume, folder))
}
