package metadata

import (
	"context"
	"fmt"
	"io"
	"path"
	"strconv"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	gometadata "github.com/mutablelogic/go-metadata"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// markdownPart is the most of a file's Markdown which is returned, from
	// its start, as a preview of it
	markdownPart = 256 << 10

	// The keys of go-metadata's Markdown: its part, and where the next starts
	markdownBody = "body"
	markdownNext = "next"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Markdown reads the file's content as Markdown, of go-metadata, such as a
// PDF's, or a Word document's, and returns its start, at most markdownPart
// of it, and where the rest starts, when there's more. A file which can't
// be read as Markdown is ErrNotImplemented.
func (self *Metadata) Markdown(ctx context.Context, r io.Reader, file *schema.Object) (_ *schema.Markdown, err error) {
	ctx, end := otel.StartSpan(self.tracer, ctx, "metadata.Markdown", attribute.String("volume", file.Name), attribute.String("path", file.Path))
	defer func() { end(err) }()

	opts := append(self.extractOpts(file, false), gometadata.WithNamespace(gometadata.MarkdownNamespace), gometadata.WithRange(fmt.Sprintf("0:%d", markdownPart)))
	_, entries, extractErr := gometadata.Extract(ctx, withName(r, path.Base(file.Path)), opts...)
	var result *schema.Markdown
	for _, entry := range entries {
		if entry.NS() != gometadata.MarkdownNamespace {
			continue
		}
		if result == nil {
			result = new(schema.Markdown)
		}
		switch entry.Key() {
		case markdownBody:
			result.Body = entry.Value()
		case markdownNext:
			if next, err := strconv.ParseUint(entry.Value(), 10, 64); err == nil {
				result.Next = next
			}
		}
	}
	switch {
	case result != nil:
		return result, nil
	case extractErr != nil && !isNotImplemented(extractErr):
		return nil, extractErr
	default:
		return nil, gofiler.ErrNotImplemented.Withf("%q can't be read as Markdown", file.Path)
	}
}
