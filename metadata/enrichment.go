// Package metadata is the task which enriches the files of a volume which
// is indexed with index=metadata, with their metadata, of go-metadata: tags
// of audio and video, EXIF of photos, the titles of documents, movies and
// TV episodes on TMDB, recordings on AcoustID, and so on. Which files are
// read depends on the build: without cgo, those of go-metadata's handlers
// which are pure Go, and with it, audio, video, and photos' EXIF, HEIF and
// RAW, too, which need FFmpeg 8, libexif, libheif and LibRaw.
package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/indexer"
	schema "github.com/mutablelogic/go-filer/schema"
	gometadata "github.com/mutablelogic/go-metadata"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Metadata enriches files with their metadata, of go-metadata
type Metadata struct {
	opt
}

var _ indexer.Task = (*Metadata)(nil).Enrich

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// identifier is a namespace, or a key, of metadata, which the store keeps
var identifier = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{0,63}$`)

// skipped are the namespaces of go-metadata which aren't metadata of a
// file: its content, as Markdown, which a content task reads, and its
// artwork, which is stored as artwork (see artwork)
var skipped = map[string]bool{gometadata.MarkdownNamespace: true, gometadata.ArtworkNamespace: true}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New returns the task which enriches files with their metadata, with the
// options, of which the server's command line is
func New(opts ...Opt) (*Metadata, error) {
	self := new(Metadata)
	if err := self.opt.apply(opts); err != nil {
		return nil, err
	}
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Enrich reads the file of the backend, and stores its metadata, of
// go-metadata, in the store, which replaces what was found of it before,
// and its artwork, such as an album's cover, or a thumbnail, when the
// store's volume is enriched with its artwork, of the same read. A file
// whose type no handler reads has the metadata of its name, such as its
// title. It's an indexer.Task, of indexer.WithMetadataTask.
func (self *Metadata) Enrich(ctx context.Context, backend gofiler.Filer, store indexer.MetadataStore, object *schema.Object) (err error) {
	ctx, end := otel.StartSpan(self.tracer, ctx, "metadata.Enrich", attribute.String("volume", object.Name), attribute.String("path", object.Path))
	defer func() { end(err) }()

	read, file, err := backend.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Name: object.Name, Path: object.Path}})
	if err != nil {
		return err
	}
	// The metadata which is extracted is stored, even when a handler fails,
	// such as a lookup on TMDB, and then the error is returned, so the task
	// is done again, but a file whose type no handler reads isn't an error
	var entries []gometadata.Metadata
	var extractErr error
	if err := read(ctx, func(r io.Reader) error {
		_, entries, extractErr = gometadata.Extract(ctx, withName(r, path.Base(file.Path)), self.extractOpts(file, store.Artwork())...)
		return nil
	}); err != nil {
		return err
	}
	found := schema.TaskMeta{Meta: meta(entries), Geo: shapes(entries), Events: occurrences(entries)}
	if store.Artwork() {
		found.Artwork = artwork(entries)
	}
	if err := store.SetMetadata(ctx, object.Path, found); err != nil {
		return err
	} else if extractErr != nil && !isNotImplemented(extractErr) {
		if errors.Is(extractErr, gometadata.ErrBadParameter) {
			return unreadable{extractErr}
		}
		return extractErr
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// extractOpts returns the options of go-metadata of a file: of the task,
// the file's type, when it's known, and its artwork, as well as all of its
// metadata, when it's asked for, which go-metadata only finds then, as it's
// slow to
func (self *Metadata) extractOpts(file *schema.Object, artwork bool) []gometadata.Option {
	opts := append([]gometadata.Option(nil), self.opts...)
	if artwork {
		opts = append(opts, gometadata.WithNamespace(gometadata.MetadataNamespace, gometadata.ArtworkNamespace))
	}
	if file.ContentType != "" && file.ContentType != "application/octet-stream" {
		opts = append(opts, gometadata.WithContentType(file.ContentType))
	}
	return opts
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// meta returns the metadata of go-metadata's entries, which the store keeps:
// those which are values, of JSON, but not images, or bytes, such as
// artwork, which an artwork task reads, nor content, of namespaces and keys
// which are identifiers. A key which has more than one value, such as the
// artists of a recording, is one entry, of an array of its values, in the
// order they're found, without those which are repeated, as the store keeps
// one value of each namespace and key. Its text is go-metadata's, such as 5s
// of a duration, when it says more than the value (see text), and of more
// than one value, their texts, between commas.
func meta(entries []gometadata.Metadata) []schema.Meta {
	result := make([]schema.Meta, 0, len(entries))
	values := make(map[string][]json.RawMessage, len(entries))
	texts := make(map[string][]string, len(entries))
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
		key := entry.NS() + ":" + entry.Key()
		if _, exists := values[key]; !exists {
			result = append(result, schema.Meta{Namespace: entry.NS(), Key: entry.Key(), Value: value})
		} else if slices.ContainsFunc(values[key], func(v json.RawMessage) bool { return bytes.Equal(v, value) }) {
			continue
		}
		values[key] = append(values[key], value)
		texts[key] = append(texts[key], text(entry, value))
	}
	for i, m := range result {
		key := m.Namespace + ":" + m.Key
		if v := values[key]; len(v) > 1 {
			if array, err := json.Marshal(v); err == nil {
				result[i].Value = array
			}
		}
		if t := texts[key]; slices.ContainsFunc(t, func(s string) bool { return s != "" }) {
			for j, s := range t {
				if s == "" {
					t[j] = plain(values[key][j])
				}
			}
			result[i].Text = strings.Join(t, ", ")
		}
	}
	return result
}

// text returns an entry's text, of go-metadata, such as 5s of a duration,
// or "" when it says no more than its value: a value which is its text, of a
// string, or of JSON, such as a number, a list, which is shown of its
// values, or a time, which is shown in the reader's own time
func text(entry gometadata.Metadata, value json.RawMessage) string {
	t := strings.TrimSpace(entry.Value())
	if _, isTime := entry.Any().(time.Time); isTime || t == "" || t == plain(value) {
		return ""
	} else if len(value) > 0 && value[0] == '[' {
		return ""
	}
	return t
}

// plain returns a value of JSON as it reads: a string, without its quotes,
// or else its JSON, such as null
func plain(value json.RawMessage) string {
	var s string
	if len(value) > 0 && value[0] == '"' && json.Unmarshal(value, &s) == nil {
		return s
	}
	return string(value)
}

// isNotImplemented returns true when go-metadata has no handler of a file's
// type, which isn't an error of the task, as the file has the metadata of
// its name
func isNotImplemented(err error) bool {
	return errors.Is(err, gometadata.ErrNotImplemented)
}

// named is a reader with a name, whose extension go-metadata detects a
// file's type of, and which a handler can use, such as to look a movie up
type named struct {
	io.Reader
	name string
}

// namedFile is a reader with a name, which can seek, and be read at an
// offset, as a file can, so go-metadata reads it in place, rather than
// copying it first
type namedFile struct {
	named
	io.ReaderAt
	io.Seeker
}

func (n named) Name() string { return n.name }

// withName returns the reader with a name, which can seek, and be read at
// an offset, when the reader can
func withName(r io.Reader, name string) gometadata.NamedReader {
	n := named{Reader: r, name: name}
	ra, readsAt := r.(io.ReaderAt)
	s, seeks := r.(io.Seeker)
	if readsAt && seeks {
		return namedFile{named: n, ReaderAt: ra, Seeker: s}
	}
	return n
}

// unreadable is an error of a file which go-metadata can't read, as it
// isn't valid, of its ErrBadParameter, which is go-filer's ErrBadParameter
// too, so the task is given up at once, rather than done again, of its
// message
type unreadable struct {
	error
}

func (u unreadable) Unwrap() error {
	return u.error
}

func (u unreadable) Is(target error) bool {
	return target == gofiler.ErrBadParameter
}
