package indexer

import (
	"context"
	"io"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// folderIterator lists folders, a page at a time, from the index, or, until
// it's ready, the folders of the backend's listing
type folderIterator struct {
	indexer *Indexer
	req     store.ListRequest
	page    []*schema.Object
	done    bool

	backend schema.ListObjectsIterator
}

var _ schema.ListObjectsIterator = (*folderIterator)(nil)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Next returns the next folder, or io.EOF when there are no more
func (self *folderIterator) Next(ctx context.Context) (*schema.Object, error) {
	if self.backend != nil {
		for {
			object, err := self.backend.Next(ctx)
			if err != nil || object.IsDir {
				return object, err
			}
		}
	}
	for {
		if len(self.page) > 0 {
			object := self.page[0]
			self.page = self.page[1:]
			if !object.IsDir {
				continue
			}
			object.Name = self.indexer.name
			return object, nil
		} else if self.done {
			return nil, io.EOF
		}
		page, err := self.indexer.store.ListObjects(ctx, self.indexer.name, self.req)
		if err != nil {
			return nil, err
		}
		if len(page) < self.req.Limit || len(page) == 0 {
			self.done = true
		}
		if len(page) > 0 {
			self.req.After = store.Key(page[len(page)-1])
		}
		self.page = page
	}
}

// Close ends the listing
func (self *folderIterator) Close() error {
	if self.backend != nil {
		return self.backend.Close()
	}
	return nil
}

// objectIterator lists objects, a page at a time, from the index, of the
// request, or of the page, when it's done, which is of a file, which it
// counts
type objectIterator struct {
	indexer *Indexer
	req     store.ListRequest
	page    []*schema.Object
	done    bool
	file    bool
}

var _ schema.ListObjectsIterator = (*objectIterator)(nil)
var _ schema.Counter = (*objectIterator)(nil)

// Count returns how many objects the listing has, of every page, without
// where it starts after
func (self *objectIterator) Count(ctx context.Context) (uint64, error) {
	if self.file {
		return 1, nil
	}
	return self.indexer.store.CountObjects(ctx, self.indexer.name, self.req)
}

// Next returns the next object, or io.EOF when there are no more
func (self *objectIterator) Next(ctx context.Context) (*schema.Object, error) {
	for {
		if len(self.page) > 0 {
			object := self.page[0]
			self.page = self.page[1:]
			object.Name = self.indexer.name
			return object, nil
		} else if self.done {
			return nil, io.EOF
		} else if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := self.indexer.store.ListObjects(ctx, self.indexer.name, self.req)
		if err != nil {
			return nil, err
		}
		if len(page) < self.req.Limit || len(page) == 0 {
			self.done = true
		}
		if len(page) > 0 {
			self.req.After = store.Key(page[len(page)-1])
		}
		self.page = page
	}
}

// Close ends the listing
func (self *objectIterator) Close() error {
	self.page, self.done = nil, true
	return nil
}
