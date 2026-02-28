package backend

import (
	"context"
	"io"
	"net/url"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
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
	ReadObject(context.Context, schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error)

	// List objects in the backend
	ListObjects(context.Context, schema.ListObjectsRequest) (*schema.ListObjectsResponse, error)

	// Delete objects in the backend (single object or prefix)
	DeleteObjects(context.Context, schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error)

	// Delete a single object from the backend
	DeleteObject(context.Context, schema.DeleteObjectRequest) (*schema.Object, error)
}
