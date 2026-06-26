package video

import (
	"bytes"
	"context"
	"io"
	"regexp"
	"strings"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
	imageutil "github.com/mutablelogic/go-filer/metadata/image"
	ffmpeg "github.com/mutablelogic/go-media/pkg/ffmpeg"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type videoextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(videoextractor))
	ffmpeg.SetLogging(false, func(_ string) {
		// Supress logging
	})
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *videoextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`video/.*`)
}

func (e *videoextractor) ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.Meta, []*schema.ArtworkMeta, error) {
	reader, err := ffmpeg.NewReader(r)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()

	// Add duration
	kv := schema.AppendMeta([]schema.Meta{}, metadata.VideoDurationSecs, reader.Duration().Seconds())
	kv = schema.AppendMeta(kv, metadata.VideoDuration, reader.Duration().Truncate(time.Second).String())

	// Add other metadata
	for _, meta := range reader.Metadata() {
		if key := sanitizeKey(meta.Key()); key != "" {
			kv = schema.AppendMeta(kv, key, meta.Value())
		}
	}

	// Extract embedded artwork, thumbnailing each via CreateArtwork
	var artwork []*schema.ArtworkMeta
	for _, meta := range reader.Metadata(ffmpeg.MetaArtwork) {
		art, _, err := imageutil.CreateArtwork(bytes.NewReader(meta.Bytes()))
		if err != nil {
			continue
		}
		artwork = append(artwork, art)
	}

	return kv, artwork, nil
}

func sanitizeKey(key string) string {
	key = strings.ToLower(key)

	// Replace any non-alphanumeric characters with underscores
	key = regexp.MustCompile(`\W+`).ReplaceAllString(key, "-")

	// Replace underscores with dashes
	key = strings.ReplaceAll(key, "_", "-")

	// Supress common keys that are not useful
	switch key {
	case "compatible-brands", "major-brand", "minor-version", "comment":
		return ""
	case "itunes-cddb-1", "itunmovi", "gapless-playback", "itunextc":
		return ""
	case "title":
		return metadata.VideoTitle
	case "director":
		return metadata.VideoDirector
	case "description":
		return metadata.VideoDescription
	case "synopsis":
		return metadata.VideoSynopsis
	case "date", "year":
		return metadata.VideoYear
	}

	// Return lowercase and trim any leading/trailing underscores
	return strings.Trim(key, "-")
}
