package video

import (
	"context"
	"regexp"
	"strings"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	metadata "github.com/mutablelogic/go-filer/metadata"
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

func (e *videoextractor) ExtractMetadata(ctx context.Context, path string) ([]schema.MetadataKV, error) {
	reader, err := ffmpeg.Open(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Add duration
	kv := make([]schema.MetadataKV, 0, 2)
	if duration := reader.Duration(); duration > 0 {
		kv = schema.AppendMetadataKV(kv, metadata.VideoDurationSecs, duration.Seconds())
		kv = schema.AppendMetadataKV(kv, metadata.VideoDuration, duration.Truncate(time.Second).String())
	}

	// Add other metadata
	for _, meta := range reader.Metadata() {
		if key := sanitizeKey(meta.Key()); key != "" {
			kv = schema.AppendMetadataKV(kv, "video-"+key, meta.Value())
		}
	}

	return kv, nil
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
