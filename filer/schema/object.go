package schema

import (
	"io"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type CreateObjectRequest struct {
	Path        string
	Body        io.Reader  `json:"-"`
	ContentType string     // optional: MIME type of the object
	ModTime     time.Time  // optional: modification time (stored as metadata)
	Meta        ObjectMeta // optional: user-defined metadata
	IfNotExists bool       // if true, fail with ErrConflict when the object already exists
}

// ObjectMeta is a string key-value map for user-defined object metadata.
// Keys should be lowercase for S3 compatibility, as S3 normalizes all
// metadata keys to lowercase.
type ObjectMeta map[string]string

type ObjectKey struct {
	Name string `json:"name,omitempty"`
	Path string `json:"path,omitempty"`
}

// Object represents a single stored item returned by the API.
type Object struct {
	Name        string     `json:"name,omitempty"`
	Path        string     `json:"path,omitempty"`
	IsDir       bool       `json:"dir,omitempty"`
	Size        int64      `json:"size"`
	ModTime     time.Time  `json:"last-modified,omitzero"`
	ContentType string     `json:"type,omitempty"`
	ETag        string     `json:"etag,omitempty"`
	Meta        ObjectMeta `json:"meta,omitempty"`
}

type ListObjectsRequest struct {
	pg.OffsetLimit
	Path      *string `json:"path,omitempty"`      // optional path prefix within the backend
	Recursive bool    `json:"recursive,omitempty"` // if true, list all objects recursively; if false, list only immediate children
}

type ObjectList struct {
	ListObjectsRequest
	Count int       `json:"count"`          // total number of matching objects, before offset/limit
	Body  []*Object `json:"body,omitempty"` // page of objects; nil when Limit==0 (count-only)
}

type GetObjectRequest struct {
	Path string
}

type ReadObjectRequest struct {
	GetObjectRequest
}

type DeleteObjectRequest struct {
	Path string
}

type DeleteObjectsRequest struct {
	Path      string `json:"path,omitempty"`
	Recursive bool   `json:"recursive,omitempty"` // if true, delete all objects recursively; if false, delete only immediate children
}

type DeleteObjectsResponse struct {
	Name string   `json:"name,omitempty"`
	Body []Object `json:"body,omitempty"` // list of deleted objects
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (o Object) String() string {
	return types.Stringify(o)
}

func (k ObjectKey) String() string {
	return types.Stringify(k)
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

func (r ObjectList) String() string {
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
