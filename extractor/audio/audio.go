package audio

import (
	"context"
	"regexp"
	"strings"
	"time"

	// Packages
	extractor "github.com/mutablelogic/go-filer/extractor"
	registry "github.com/mutablelogic/go-filer/extractor/registry"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	ffmpeg "github.com/mutablelogic/go-media/pkg/ffmpeg"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type audioextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	registry.RegisterExtractor(new(audioextractor))
	ffmpeg.SetLogging(false, func(_ string) {
		// Supress logging
	})
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *audioextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`audio/.*`)
}

func (e *audioextractor) ExtractMetadata(ctx context.Context, path string) ([]schema.MetadataKV, error) {
	reader, err := ffmpeg.Open(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Add duration
	metadata := make([]schema.MetadataKV, 0, 2)
	if duration := reader.Duration(); duration > 0 {
		metadata = schema.AppendMetadataKV(metadata, extractor.AudioDurationSecs, duration.Seconds())
		metadata = schema.AppendMetadataKV(metadata, extractor.AudioDuration, duration.Truncate(time.Second).String())
	}

	// Add other metadata
	for _, meta := range reader.Metadata() {
		if key := sanitizeKey(meta.Key()); key != "" {
			metadata = schema.AppendMetadataKV(metadata, "audio-"+key, meta.Value())
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
	case "eitunnorm", "tagging-time", "accurateripdiscid", "accurateripresult", "comment", "id3v1-comment":
		return ""
	case "id3v2-priv-averagelevel", "id3v2-priv-google-originalclientid", "id3v2-priv-www-amazon-com":
		return ""
	case "itunes-cddb-1", "itunmovi", "itunnorm", "itunsmpb", "gapless-playback", "itunextc", "compatible-brands", "itunes-cddb-ids":
		return ""
	case "account-id", "major-brand", "minor-version":
		return ""
	case "itunes-cddb-tracknumber":
		return "track"
	case "artists", "artist", "album-artist":
		return "artist"
	case "originalyear", "year", "date", "originaldate", "tdor":
		return "year"
	}

	// Return lowercase and trim any leading/trailing underscores
	return strings.Trim(key, "-")
}
