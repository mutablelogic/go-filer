package indexer

import (
	"context"
	"io"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// folderIterator lists folders, a page at a time, from the index, or, until
// it's ready, the folders of the backend's listing
type folderIterator struct {
	indexer *Indexer
	req     ListRequest
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
		page, err := self.indexer.store.List(ctx, self.indexer.name, self.req)
		if err != nil {
			return nil, err
		}
		if len(page) < self.req.Limit || len(page) == 0 {
			self.done = true
		}
		if len(page) > 0 {
			self.req.After = Key(page[len(page)-1])
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
