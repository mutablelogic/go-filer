package filer

import (
	"context"
	"io"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// CONSTANTS

const (
	// AttrLastModified is the metadata key used to store the object modification time
	// Note: S3 normalizes metadata keys to lowercase, so we use lowercase for consistency
	AttrLastModified = "last-modified"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Filer interface {
	io.Closer

	// Name returns the name of the filer backend
	Name() string

	// Key returns the storage key for a given path within this filer.
	// Returns empty string if the path is not handled by this filer (e.g., prefix mismatch).
	// Returns "/" for the root.
	Key(path string) string

	// Create object in the filer
	CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error)

	// Get object metadata from the filer
	GetObject(context.Context, schema.GetObjectRequest) (*schema.Object, error)

	// Read object content from the filer. Caller must close the returned reader.
	ReadObject(context.Context, schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error)

	// List objects in the filer
	ListObjects(context.Context, schema.ListObjectsRequest) (*schema.ListObjectsResponse, error)

	// Delete objects in the backend (single object or prefix)
	DeleteObjects(context.Context, schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error)

	// Delete a single object from the filer
	DeleteObject(context.Context, schema.DeleteObjectRequest) (*schema.Object, error)
}
