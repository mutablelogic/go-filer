package metadata

import (
	"context"
	"io"
	"regexp"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Extractor interface {
	// Return the media type
	MediaType() *regexp.Regexp

	// Extract metadata from the file at the given path
	ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.Meta, error)
}

type FileReader interface {
	io.Reader
	Name() string
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	extractors = make(map[*regexp.Regexp]Extractor)
)

func RegisterExtractor(e Extractor) {
	extractors[e.MediaType()] = e
}

////////////////////////////////////////////////////////////////////////////////
// METADATA KEYS

const (
	ImageFormat       = "format"
	ImageWidth        = "width"
	ImageHeight       = "height"
	ImageTitle        = "title"
	ImageSummary      = "summary"
	ImageTags         = "tags"
	TextTitle         = "title"
	TextAuthor        = "author"
	TextSummary       = "summary"
	TextLanguage      = "language"
	TextLines         = "lines"
	TextTags          = "tags"
	PDFTitle          = "title"
	PDFAuthor         = "author"
	PDFSubject        = "subject"
	PDFCreator        = "creator"
	PDFProducer       = "producer"
	PDFPages          = "pages"
	VideoDurationSecs = "duration-secs"
	VideoDuration     = "duration"
	VideoDescription  = "description"
	VideoSynopsis     = "synopsis"
	VideoTitle        = "title"
	VideoDirector     = "director"
	VideoYear         = "year"
	AudioDurationSecs = "duration-secs"
	AudioDuration     = "duration"
	AudioTitle        = "title"
	AudioArtist       = "artist"
	AudioAlbum        = "album"
	AudioGenre        = "genre"
	AudioYear         = "year"
	AudioTrack        = "track"
	DateCreated       = "created"
	DateModified      = "modified"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Return metadata for a *schema.Object in a specific backend
func Get(mimeType string) (Extractor, error) {
	for re, e := range extractors {
		if !re.MatchString(mimeType) {
			continue
		}
		return e, nil
	}
	return nil, gofiler.ErrNotImplemented.Withf("no extractor registered for media type %q", mimeType)
}
