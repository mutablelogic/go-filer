package video

import (
	"context"
	"regexp"
	"strings"
	"time"

	// Packages
	"github.com/mutablelogic/go-filer/extractor"
	registry "github.com/mutablelogic/go-filer/extractor/registry"
	schema "github.com/mutablelogic/go-filer/extractor/schema"
	ffmpeg "github.com/mutablelogic/go-media/pkg/ffmpeg"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type videoextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	registry.RegisterExtractor(new(videoextractor))
	ffmpeg.SetLogging(false, func(_ string) {
		// Supress logging
	})
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *videoextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`video/.*`)
}

func (e *videoextractor) ExtractMetadata(ctx context.Context, path string) ([]schema.MetadataKV, error) {
	reader, err := ffmpeg.Open(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Add duration
	metadata := make([]schema.MetadataKV, 0, 2)
	if duration := reader.Duration(); duration > 0 {
		metadata = schema.AppendMetadataKV(metadata, extractor.VideoDurationSecs, duration.Seconds())
		metadata = schema.AppendMetadataKV(metadata, extractor.VideoDuration, duration.Truncate(time.Second).String())
	}

	// Add other metadata
	for _, meta := range reader.Metadata() {
		if key := sanitizeKey(meta.Key()); key != "" {
			metadata = schema.AppendMetadataKV(metadata, "video-"+key, meta.Value())
		}
	}

	return metadata, nil
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
	case "date":
		return "year"
	}

	// Return lowercase and trim any leading/trailing underscores
	return strings.Trim(key, "-")
}
