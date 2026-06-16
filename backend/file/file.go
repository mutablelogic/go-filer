package file

import (
	"context"
	"io"
	"net/url"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	backend "github.com/mutablelogic/go-filer/backend"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type FileBackend struct {
	name string
	path string
}

var _ backend.Backend = (*FileBackend)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(url *url.URL) (*FileBackend, error) {
	self := new(FileBackend)

	name, err := Validate(url)
	if err != nil {
		return nil, err
	} else {
		self.name = name
		self.path = url.Path
	}

	return self, nil
}

func (FileBackend) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Name returns the name of the backend
func (self *FileBackend) Name() string {
	return self.name
}

// URL returns the backend destination URL. The scheme, host (bucket/name),
// and path (prefix/directory) identify the storage location. Query
// parameters carry useful non-credential details: region, endpoint, anonymous.
func (self *FileBackend) URL() *url.URL {
	url := new(url.URL)
	url.Scheme = "file"
	url.Host = self.name
	url.Path = self.path
	return url
}

// Create object in the backend
func (FileBackend) CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error) {
	return nil, gofiler.ErrNotImplemented
}

// Get object metadata from the backend
func (FileBackend) GetObject(context.Context, schema.GetObjectRequest) (*schema.Object, error) {
	return nil, gofiler.ErrNotImplemented
}

// Read object content from the backend. Caller must close the returned reader.
func (FileBackend) ReadObject(context.Context, schema.ReadObjectRequest) (io.ReadCloser, *schema.Object, error) {
	return nil, nil, gofiler.ErrNotImplemented
}

// List objects in the backend
func (FileBackend) ListObjects(context.Context, schema.ListObjectsRequest) (*schema.ListObjectsResponse, error) {
	return nil, gofiler.ErrNotImplemented
}

// Delete objects in the backend (single object or prefix)
func (FileBackend) DeleteObjects(context.Context, schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	return nil, gofiler.ErrNotImplemented
}
