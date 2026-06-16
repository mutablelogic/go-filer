package manager

import (
	"context"
	"io"

	// Packages

	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opt
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new manager object
func New(ctx context.Context, opts ...Opt) (_ *Manager, err error) {
	self := new(Manager)

	// Apply options
	if err := self.opt.apply(opts); err != nil {
		return nil, err
	}

	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// GET METADATA

func (m *Manager) Get(ctx context.Context, mimeType string, r io.Reader) ([]schema.MetadataKV, error) {
	extractor, err := metadata.Get(mimeType)
	if err != nil {
		return nil, err
	}
	// Extract metadata does not produce errors, only warnings
	return extractor.ExtractMetadata(ctx, r)
}
