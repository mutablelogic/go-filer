package filer

import (
	"context"
	"io"
	"net/url"

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
