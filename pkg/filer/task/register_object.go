package task

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	ffmpeg "github.com/mutablelogic/go-media/pkg/ffmpeg"
	ref "github.com/mutablelogic/go-server/pkg/ref"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	reTrackDisc = regexp.MustCompile(`^(\d+)[/](\d+)$`)
)

var (
	audiobookGenres = []string{"Spoken Word", "Audiobook", "Audio Book", "Spoken Word", "Spoken-Word"}
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *taskrunner) RegisterObject(ctx context.Context, object *schema.Object) error {
	var errs error

	tmp, err := os.CreateTemp("", "*."+filepath.Base(object.Key))
	if err != nil {
		return err
	}

	// Let's read the object into a temporary file
	if _, err := t.filer.WriteObject(ctx, tmp, object.Bucket, object.Key); err != nil {
		return err
	} else if err := tmp.Close(); err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	// Analyze the object
	r, err := ffmpeg.Open(tmp.Name())
	if err != nil {
		return fmt.Errorf("ffmpeg: %w", err)
	}
	defer r.Close()

	// Create the media object
	media, err := t.createMedia(ctx, object, r)
	if err != nil {
		return err
	}

	ref.Log(ctx).With("media", media).Printf(ctx, "Created media")

	// Return any errors
	return errs
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (t *taskrunner) createMedia(ctx context.Context, object *schema.Object, r *ffmpeg.Reader) (*schema.Media, error) {
	var meta schema.MediaMeta

	// Set the media type
	if typ, err := types.ParseContentType(object.Type); err != nil {
		return nil, err
	} else {
		// Major part of the mimetype
		meta.Type = strings.SplitN(typ, "/", 2)[0]
	}

	// Set duration
	if duration := r.Duration(); duration > 0 {
		meta.Duration = types.DurationPtr(duration)
	}

	// Set metadata
	meta.Meta = make(map[string]any, 10)
	for _, metadata := range r.Metadata() {
		key := metadata.Key()
		meta.Meta[key] = metadata.Value()
	}

	// Set title, artist, album, etc.
	if title, exists := meta.GetMetaString("title"); exists {
		meta.Title = types.StringPtr(title)
	}
	if album, exists := meta.GetMetaString("album", "show"); exists {
		meta.Album = types.StringPtr(album)
	}
	if artist, exists := meta.GetMetaString("artist", "album_artist", "artist-sort", "sort_artist", "sort_album_artist", "artists"); exists {
		meta.Artist = types.StringPtr(artist)
	}
	if composer, exists := meta.GetMetaString("composer"); exists {
		meta.Composer = types.StringPtr(composer)
	}
	if genre, exists := meta.GetMetaString("genre"); exists {
		meta.Genre = types.StringPtr(genre)
	}

	// Track or episode number
	if track, exists := meta.GetMetaString("track", "iTunes_CDDB_TrackNumber", "episode", "episode_id"); exists {
		var trackStr string
		if match := reTrackDisc.FindStringSubmatch(track); len(match) == 3 {
			trackStr = match[1]
		} else {
			trackStr = track
		}
		if track, err := strconv.ParseUint(trackStr, 10, 64); err == nil {
			meta.Track = types.Uint64Ptr(track)
		}
	}

	// Disc or season number
	if disc, exists := meta.GetMetaString("disc", "season", "season_number"); exists {
		var discStr string
		if match := reTrackDisc.FindStringSubmatch(disc); len(match) == 3 {
			discStr = match[1]
		} else {
			discStr = disc
		}
		if disc, err := strconv.ParseUint(discStr, 10, 64); err == nil {
			meta.Disc = types.Uint64Ptr(disc)
		}
	}

	// Year or release or broadcast
	if year, exists := meta.GetMetaString("year", "originalyear", "tory", "tery", "date"); exists {
		if year, err := strconv.ParseUint(year, 10, 64); err == nil {
			if year > 1000 {
				meta.Year = types.Uint64Ptr(year)
			}
		}
	}

	// Synopsis or description
	if description, exists := meta.GetMetaString("description", "comment"); exists {
		meta.Description = types.StringPtr(description)
	}

	// Types
	if meta.Type == "audio" && meta.Genre != nil && slices.Contains(audiobookGenres, types.PtrString(meta.Genre)) {
		meta.Type = "audiobook"
	}

	// Retrieve artwork by using the MetaArtwork key. The value is of type []byte
	// which needs to be converted to an image.
	for _, artwork := range r.Metadata(ffmpeg.MetaArtwork) {
		mimetype := artwork.Value()
		if mimetype == "" {
			continue
		}
		data := artwork.Bytes()
		if data == nil {
			continue
		}
		meta.Images = append(meta.Images, schema.MediaImage{
			Type: mimetype,
			Hash: types.Hash(data),
			Data: data,
		})
	}

	// Insert the media object into the database
	media, err := t.filer.CreateMedia(ctx, object.Bucket, object.Key, meta)
	if err != nil {
		return nil, err
	}

	// Return success
	return media, nil
}
