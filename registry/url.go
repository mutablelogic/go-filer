package registry

import (
	"net/url"
	"path/filepath"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// ParseURL returns the URL of a volume's backend, which is a URL, such as
// s3://my.bucket/photos, or otherwise a directory, without a scheme, with
// any options after a "?", such as /mnt/media?readonly=true, which is made
// absolute, when it's relative, as the current directory's, and is a file
// URL, without a host, as its name isn't known, such as
// file:///mnt/media?readonly=true. On Windows, a directory with a drive,
// such as C:\media, or C:media, which is relative to the drive's current
// directory, isn't a URL whose scheme is its drive, and its URL's path has
// the drive, such as /C:/media. Returns gofiler.ErrBadParameter, without the value,
// which can have credentials, when it can't be parsed, and a value with
// "://" is a URL, not a directory, even when it can't be parsed.
func ParseURL(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	switch {
	case err == nil && len(u.Scheme) > 1 && !filepath.IsAbs(value):
		return u, nil
	case strings.Contains(value, "://"):
		// A URL which can't be parsed isn't a directory, and isn't in the
		// error, as it can have credentials
		return nil, gofiler.ErrBadParameter.With("invalid url")
	}

	// A directory, with any options
	dir, query, _ := strings.Cut(value, "?")
	path, err := filepath.Abs(dir)
	if err != nil || dir == "" {
		return nil, gofiler.ErrBadParameter.With("invalid directory")
	}
	u = &url.URL{Scheme: schema.SchemeFile, Path: filepath.ToSlash(path), RawQuery: query}
	if !strings.HasPrefix(u.Path, "/") {
		u.Path = "/" + u.Path
	}
	return u, nil
}
