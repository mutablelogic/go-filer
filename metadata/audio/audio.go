package audio

import (
	"context"
	"io"
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

type audioextractor struct{}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func init() {
	metadata.RegisterExtractor(new(audioextractor))
	ffmpeg.SetLogging(false, func(_ string) {
		// Supress logging
	})
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (e *audioextractor) MediaType() *regexp.Regexp {
	return regexp.MustCompile(`audio/.*`)
}

func (e *audioextractor) ExtractMetadata(ctx context.Context, r io.Reader) ([]schema.Meta, error) {
	reader, err := ffmpeg.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Add duration
	kv := schema.AppendMeta([]schema.Meta{}, metadata.AudioDurationSecs, reader.Duration().Seconds())
	kv = schema.AppendMeta(kv, metadata.AudioDuration, reader.Duration().Truncate(time.Second).String())

	// Add other metadata
	for _, meta := range reader.Metadata() {
		if key := sanitizeKey(meta.Key()); key != "" {
			kv = schema.AppendMeta(kv, key, meta.Value())
		}
	}

	// Return metadata
	return kv, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

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
		return metadata.AudioTrack
	case "artists", "artist", "album-artist":
		return metadata.AudioArtist
	case "album", "albumtitle":
		return metadata.AudioAlbum
	case "genre", "music-genre":
		return metadata.AudioGenre
	case "originalyear", "year", "date", "originaldate", "tdor":
		return metadata.AudioYear
	}

	// Return lowercase and trim any leading/trailing underscores
	return strings.Trim(key, "-")
}
