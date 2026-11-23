package store

import (
	"mime"
	"regexp"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// reArtworkContext is an artwork's context, such as cover, or page-1
var reArtworkContext = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// ValidateArtwork returns ErrBadParameter when artwork has no data, its
// type isn't an image's, it has no size, or its context isn't a name, of
// a letter, and up to 63 letters, digits, - and _, in lower case
func ValidateArtwork(artwork schema.ArtworkMeta) error {
	media, _, err := mime.ParseMediaType(artwork.Type)
	switch {
	case len(artwork.Data) == 0:
		return gofiler.ErrBadParameter.With("artwork has no data")
	case err != nil || !strings.HasPrefix(media, "image/"):
		return gofiler.ErrBadParameter.Withf("artwork's type isn't an image's: %q", artwork.Type)
	case artwork.Width == 0 || artwork.Height == 0:
		return gofiler.ErrBadParameter.With("artwork has no width, or height")
	case !reArtworkContext.MatchString(artwork.Context):
		return gofiler.ErrBadParameter.Withf("invalid artwork context: %q", artwork.Context)
	}
	return nil
}

// ArtworkHash returns the hash of artwork, in lower case, or
// ErrBadParameter when it isn't 64 hex digits
func ArtworkHash(hash string) (string, error) {
	hash = strings.ToLower(strings.TrimSpace(hash))
	if len(hash) != 64 || strings.Trim(hash, "0123456789abcdef") != "" {
		return "", gofiler.ErrBadParameter.Withf("invalid artwork hash: %q", hash)
	}
	return hash, nil
}
