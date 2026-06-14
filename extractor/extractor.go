package extractor

import (
	"context"
	"regexp"

	// Packages
	"github.com/mutablelogic/go-filer/extractor/schema"
)

const (
	ImageFormat       = "image-format"
	ImageWidth        = "image-width"
	ImageHeight       = "image-height"
	ImageTitle        = "image-title"
	ImageSummary      = "image-summary"
	ImageTags         = "image-tags"
	TextTitle         = "text-title"
	TextLines         = "text-lines"
	TextSummary       = "text-summary"
	TextTags          = "text-tags"
	PDFTitle          = "pdf-title"
	PDFAuthor         = "pdf-author"
	PDFSubject        = "pdf-subject"
	PDFCreator        = "pdf-creator"
	PDFProducer       = "pdf-producer"
	PDFPages          = "pdf-pages"
	VideoDurationSecs = "video-duration-secs"
	VideoDuration     = "video-duration"
	VideoDescription  = "video-description"
	VideoSynopsis     = "video-synopsis"
	VideoTitle        = "video-title"
	VideoDirector     = "video-artist"
	VideoYear         = "video-year"
	AudioDurationSecs = "audio-duration-secs"
	AudioDuration     = "audio-duration"
	AudioTitle        = "audio-title"
	AudioArtist       = "audio-artist"
	AudioAlbum        = "audio-album"
	AudioGenre        = "audio-genre"
	AudioYear         = "audio-year"
	AudioTrack        = "audio-track"
)

type Extractor interface {
	// Return the media type
	MediaType() *regexp.Regexp

	// Extract metadata from the file at the given path
	ExtractMetadata(ctx context.Context, path string) ([]schema.MetadataKV, error)

	// Extract metadata from the file at the given path
	//Extract(ctx context.Context, r io.ReaderAt, params url.Values) error
}
