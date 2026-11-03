package filer

import (
	"context"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ReadObjectFunc reads an object's content: it opens the object, calls the
// function with a reader of the content, and closes the object when the
// function returns. The reader returns an error once the context is
// cancelled, or the filer is closed. Returns the function's error, or the
// error opening or closing the object.
type ReadObjectFunc func(context.Context, func(io.Reader) error) error

// Filer is the interface for storing files and objects.
type Filer interface {
	io.Closer

	// URL returns the filer URL. The scheme, host (bucket/name),
	// and path (prefix/directory) identify the storage location. Query
	// parameters carry useful non-credential details: region, endpoint, anonymous, readonly, etc.
	URL() *url.URL

	// Clone returns a new filer of the same storage, with the options of the
	// filer, changed by the patch (see PatchValues), which is nil to keep
	// them, from what the filer has, rather than its URL, which doesn't have
	// its credentials. The filer isn't changed, and the clone is checked as a
	// new filer is, such as a bucket being accessible. The registry clones a
	// volume's filer to change its options, and to open it again when
	// watching it fails.
	Clone(context.Context, url.Values) (Filer, error)

	// Create object in the filer
	CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error)

	// Get object metadata from the filer
	GetObject(context.Context, schema.GetObjectRequest) (*schema.Object, error)

	// Read object content from the filer, by calling the returned function,
	// with the object's metadata
	ReadObject(context.Context, schema.GetObjectRequest) (ReadObjectFunc, *schema.Object, error)

	// List objects in the filer. Call Next on the returned iterator until
	// io.EOF is returned. Caller must close the returned iterator.
	ListObjects(context.Context, schema.ListObjectsRequest) (schema.ListObjectsIterator, error)

	// Delete an object in the filer, and return the deleted object. A
	// directory is only deleted when the request is recursive, in which case
	// every object beneath it is deleted too. The root of a volume cannot be
	// deleted.
	DeleteObjects(context.Context, schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error)

	// Watch for changes to objects, sending each to the channel, until the
	// context is cancelled. The channel isn't closed. It will return
	// filer.ErrNotImplemented if the filer backend does not support watching objects.
	WatchObjects(context.Context, chan<- schema.Event) error
}

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
// the drive, such as /C:/media. Returns ErrBadParameter when it can't be
// parsed.
func ParseURL(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err == nil && len(u.Scheme) > 1 && !filepath.IsAbs(value) {
		return u, nil
	}

	// A directory, with any options
	dir, query, _ := strings.Cut(value, "?")
	path, err := filepath.Abs(dir)
	if err != nil || dir == "" {
		return nil, ErrBadParameter.Withf("invalid url: %q", value)
	}
	u = &url.URL{Scheme: schema.SchemeFile, Path: filepath.ToSlash(path), RawQuery: query}
	if !strings.HasPrefix(u.Path, "/") {
		u.Path = "/" + u.Path
	}
	return u, nil
}

// PatchValues returns a copy of the options, changed by the patch: each key
// of the patch replaces the option's values with its own, or, when it has
// none, removes the option, so it's the default
func PatchValues(values, patch url.Values) url.Values {
	result := make(url.Values, len(values)+len(patch))
	for key, value := range values {
		result[key] = append([]string(nil), value...)
	}
	for key, value := range patch {
		if len(value) == 0 {
			delete(result, key)
		} else {
			result[key] = append([]string(nil), value...)
		}
	}
	return result
}
