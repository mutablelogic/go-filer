package backend

import (
	"context"
	"io"

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
