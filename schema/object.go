package schema

import (
	"io"
	"time"

	// Packages
	"github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type CreateObjectRequest struct {
	Name        string
	Path        string
	Body        io.Reader  `json:"-"`
	ContentType string     // optional: MIME type of the object
	ModTime     time.Time  // optional: modification time (stored as metadata)
	Meta        ObjectMeta // optional: user-defined metadata
}

// ObjectMeta is a string key-value map for user-defined object metadata.
// Keys should be lowercase for S3 compatibility, as S3 normalizes all
// metadata keys to lowercase.
type ObjectMeta map[string]string

type Object struct {
	Name        string     `json:"name,omitempty"`
	Path        string     `json:"path,omitempty"`
	Size        int64      `json:"size,omitempty"`
	ModTime     time.Time  `json:"modtime,omitzero"`
	ContentType string     `json:"type,omitempty"`
	ETag        string     `json:"etag,omitempty"`
	Meta        ObjectMeta `json:"meta,omitempty"`
}

type ListObjectsRequest struct {
	Name      string `json:"name,omitempty"`
	Path      string `json:"path,omitempty"`      // optional path prefix within the backend
	Recursive bool   `json:"recursive,omitempty"` // if true, list all objects recursively; if false, list only immediate children
}

type GetObjectRequest struct {
	Name string
	Path string
}

type ReadObjectRequest struct {
	Name string
	Path string
}

type ListObjectsResponse struct {
	Name string
	Body []Object
}

type DeleteObjectRequest struct {
	Name string
	Path string
}

type DeleteObjectsRequest struct {
	Name      string `json:"name,omitempty"`
	Path      string `json:"path,omitempty"`
	Recursive bool   `json:"recursive,omitempty"` // if true, delete all objects recursively; if false, delete only immediate children
}

type DeleteObjectsResponse struct {
	Name string
	Body []Object // list of deleted objects
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (o Object) String() string {
	return types.Stringify(o)
}

func (r CreateObjectRequest) String() string {
	return types.Stringify(r)
}

func (r ListObjectsRequest) String() string {
	return types.Stringify(r)
}

func (r GetObjectRequest) String() string {
	return types.Stringify(r)
}

func (r ReadObjectRequest) String() string {
	return types.Stringify(r)
}

func (r ListObjectsResponse) String() string {
	return types.Stringify(r)
}

func (r DeleteObjectRequest) String() string {
	return types.Stringify(r)
}

func (r DeleteObjectsRequest) String() string {
	return types.Stringify(r)
}

func (r DeleteObjectsResponse) String() string {
	return types.Stringify(r)
}
