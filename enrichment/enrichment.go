// Package enrichment is the task which enriches the files of a volume which
// is indexed with index=metadata, with their metadata, of go-metadata: tags
// of audio and video, EXIF of photos, the titles of documents, movies and
// TV episodes on TMDB, recordings on AcoustID, and so on. Which files are
// read depends on the build: without cgo, those of go-metadata's handlers
// which are pure Go, and with it, audio, video, and photos' EXIF, HEIF and
// RAW, too, which need FFmpeg 8, libexif, libheif and LibRaw.
package enrichment

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path"
	"regexp"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/indexer"
	schema "github.com/mutablelogic/go-filer/schema"
	metadata "github.com/mutablelogic/go-metadata"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Enrichment enriches files with their metadata, of go-metadata
type Enrichment struct {
	opt
}

var _ indexer.Task = (*Enrichment)(nil).Enrich

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// identifier is a namespace, or a key, of metadata, which the store keeps
var identifier = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{0,63}$`)

// skipped are the namespaces of go-metadata which aren't metadata of a
// file: its content, as Markdown, which a content task reads
var skipped = map[string]bool{metadata.MarkdownNamespace: true}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns the task which enriches files with their metadata, with the
// options, of which the server's command line is
func New(opts ...Opt) (*Enrichment, error) {
	self := new(Enrichment)
	if err := self.opt.apply(opts); err != nil {
		return nil, err
	}
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Enrich reads the file of the backend, and stores its metadata, of
// go-metadata, in the store, which replaces what was found of it before. A
// file whose type no handler reads has the metadata of its name, such as
// its title. It's an indexer.Task, of indexer.WithMetadataTask.
func (self *Enrichment) Enrich(ctx context.Context, backend gofiler.Filer, store indexer.MetadataStore, object *schema.Object) (err error) {
	ctx, end := otel.StartSpan(self.tracer, ctx, "enrichment.Enrich", attribute.String("volume", object.Name), attribute.String("path", object.Path))
	defer func() { end(err) }()

	read, file, err := backend.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Name: object.Name, Path: object.Path}})
	if err != nil {
		return err
	}
	var entries []metadata.Metadata
	if err := read(ctx, func(r io.Reader) error {
		_, result, err := metadata.Extract(ctx, named{Reader: r, name: path.Base(file.Path)}, self.extractOpts(file)...)
		entries = result
		if err != nil && len(result) == 0 && !isNotImplemented(err) {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return store.SetMetadata(ctx, object.Path, meta(entries))
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// extractOpts returns the options of go-metadata of a file: of the task,
// and the file's type, when it's known
func (self *Enrichment) extractOpts(file *schema.Object) []metadata.Option {
	opts := append([]metadata.Option(nil), self.opts...)
	if file.ContentType != "" && file.ContentType != "application/octet-stream" {
		opts = append(opts, metadata.WithContentType(file.ContentType))
	}
	return opts
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// meta returns the metadata of go-metadata's entries, which the store keeps:
// those which are values, of JSON, but not images, or bytes, such as
// artwork, which an artwork task reads, nor content, of namespaces and keys
// which are identifiers
func meta(entries []metadata.Metadata) []schema.Meta {
	result := make([]schema.Meta, 0, len(entries))
	for _, entry := range entries {
		if skipped[entry.NS()] || !identifier.MatchString(entry.NS()) || !identifier.MatchString(entry.Key()) {
			continue
		} else if entry.Image() != nil {
			continue
		} else if _, isBytes := entry.Any().([]byte); isBytes {
			continue
		}
		value, err := json.Marshal(entry.Any())
		if err != nil {
			continue
		}
		result = append(result, schema.Meta{Namespace: entry.NS(), Key: entry.Key(), Value: value})
	}
	return result
}

// isNotImplemented returns true when go-metadata has no handler of a file's
// type, which isn't an error of the task, as the file has the metadata of
// its name
func isNotImplemented(err error) bool {
	return errors.Is(err, metadata.ErrNotImplemented)
}

// named is a reader with a name, whose extension go-metadata detects a
// file's type of, and which a handler can use, such as to look a movie up
type named struct {
	io.Reader
	name string
}

func (n named) Name() string { return n.name }
