package filer

import (
	"context"
	"io"
	"net/url"

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

	// URL returns the URL for the filer root
	URL() *url.URL

	// Path returns the path for the filer for a given URL or an empty string if the URL is not handled by this filer
	Path(url *url.URL) string

	// Handles returns true if this filer handles the given URL
	Handles(url *url.URL) bool

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
}
