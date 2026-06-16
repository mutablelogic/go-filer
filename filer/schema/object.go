package schema

import (
	"encoding/json"
	"io"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type CreateObjectRequest struct {
	Body        io.Reader `json:"-"`
	IfNotExists bool      // if true, fail with ErrConflict when the object already exists
	ObjectMeta
}

// Meta is a string key-value map for user-defined object metadata.
// Keys should be lowercase for S3 compatibility, as S3 normalizes all
// metadata keys to lowercase.
type Meta map[string]json.RawMessage

type ObjectKey struct {
	Volume string `json:"volume,omitempty"`
	Path   string `json:"path,omitempty"`
}

// Object represents a single stored item returned by the API.
type Object struct {
	ObjectKey
	ObjectMeta
	Size    int64     `json:"size"`
	ETag    *string   `json:"etag,omitempty"`
	ModTime time.Time `json:"last-modified,omitzero"`
	IsDir   bool      `json:"dir,omitempty"`
}

// ObjectMeta represents the metadata of an object, which can be updated.
type ObjectMeta struct {
	ContentType string `json:"type,omitempty"`
	Meta        Meta   `json:"meta,omitempty"`
}

// ObjectCreate represents the result of creating an object in the database as part of indexing
type ObjectCreate struct {
	ObjectKey
	ObjectMeta
	Size int64 `json:"size"`
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
	Volume string   `json:"volume,omitempty"`
	Body   []Object `json:"body,omitempty"` // list of deleted objects
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
