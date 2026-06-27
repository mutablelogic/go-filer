package backend

import (
	"context"
	"encoding/json"
	"io"
	"net/url"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Backend is the interface for a storage backend.
type Backend interface {
	io.Closer

	// Name returns the name of the backend
	Name() string

	// URL returns the backend destination URL. The scheme, host (bucket/name),
	// and path (prefix/directory) identify the storage location. Query
	// parameters carry useful non-credential details: region, endpoint, anonymous.
	URL() *url.URL

	// Create object in the backend
	CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error)

	// Get object metadata from the backend
	GetObject(context.Context, schema.GetObjectRequest) (*schema.Object, error)

	// Read object content from the backend. Caller must close the returned reader.
	ReadObject(context.Context, schema.GetObjectRequest) (io.ReadCloser, *schema.Object, error)

	// Iterate through the list of objects in the backend, until io.EOF is returned.
	ListObjects(context.Context, *schema.ObjectListIterator) error

	// Delete objects in the backend (single object or prefix)
	DeleteObjects(context.Context, schema.DeleteObjectsRequest) error
}

// DecryptCredentailFunc is a function that decrypts a credential with the given
// key string and returns the decrypted value or an error.
type DecryptCredentailFunc func(context.Context, string) (json.RawMessage, error)
