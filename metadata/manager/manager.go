package manager

import (
	"bytes"
	"context"
	"io"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
	mime "github.com/mutablelogic/go-filer/metadata/mime"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opt
}

type fileReader struct {
	reader io.Reader
	name   string
}

func (r fileReader) Read(p []byte) (int, error) {
	if r.reader == nil {
		return 0, io.ErrUnexpectedEOF
	}
	return r.reader.Read(p)
}

func (r fileReader) Name() string {
	return r.name
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

func (m *Manager) GetMeta(ctx context.Context, r io.Reader) (_ *schema.ObjectMeta, err error) {
	ctx, endSpan := otel.StartSpan(m.tracer, ctx, "GetMeta")
	defer func() { endSpan(err) }()
	if r == nil {
		return nil, gofiler.ErrBadParameter.With("reader is required")
	}

	// Extract the name from r if it implements FileReader, otherwise use a default name for MIME sniffing.
	var name string
	if fr, ok := r.(metadata.FileReader); ok {
		name = fr.Name()
	}

	// Buffer bytes consumed during MIME sniffing so we can reconstruct
	// the original stream for metadata extraction without requiring io.Seeker.
	var sniffed bytes.Buffer
	sniffReader := io.TeeReader(r, &sniffed)

	// Get the mimetype from the reader
	var result schema.ObjectMeta
	result.ContentType, _, err = mime.Type(fileReader{reader: sniffReader, name: name})
	if err != nil {
		return nil, err
	}

	// Rewind by replaying sniffed bytes first, then the unread remainder.
	if meta, _, err := m.Get(ctx, result.ContentType, fileReader{
		reader: io.MultiReader(bytes.NewReader(sniffed.Bytes()), r),
		name:   name,
	}); err != nil {
		return nil, err
	} else {
		result.Meta = meta
	}

	// Return success
	return &result, nil
}

func (m *Manager) Get(ctx context.Context, mimeType string, r io.Reader) ([]schema.Meta, []*schema.ArtworkMeta, error) {
	extractor, err := metadata.Get(mimeType)
	if err != nil {
		// It's only a warning if we couldn't find an extractor
		return []schema.Meta{}, nil, err
	}
	// Extract metadata does not produce errors, only warnings
	return extractor.ExtractMetadata(ctx, r)
}
