package schema

import (
	"io"
	"time"

	// Packages
	"github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// CONSTANTS

// MaxListLimit is the maximum number of objects that can be returned in a
// single ListObjects call. Clients must paginate using Offset for larger sets.
const MaxListLimit = 1000

////////////////////////////////////////////////////////////////////////////////
// TYPES

type CreateObjectRequest struct {
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
	Size        int64      `json:"size"`
	ModTime     time.Time  `json:"modtime,omitzero"`
	ContentType string     `json:"type,omitempty"`
	ETag        string     `json:"etag,omitempty"`
	Meta        ObjectMeta `json:"meta,omitempty"`
}

type ListObjectsRequest struct {
	Path      string `json:"path,omitempty"`      // optional path prefix within the backend
	Recursive bool   `json:"recursive,omitempty"` // if true, list all objects recursively; if false, list only immediate children
	Offset    int    `json:"offset,omitempty"`    // number of objects to skip before returning results
	Limit     int    `json:"limit,omitempty"`     // max objects to return; 0 returns the count only, no body
}

type GetObjectRequest struct {
	Path string
}

type ReadObjectRequest struct {
	GetObjectRequest
}

type ListObjectsResponse struct {
	Name  string   `json:"name,omitempty"`
	Count int      `json:"count"`          // total number of matching objects, before offset/limit
	Body  []Object `json:"body,omitempty"` // page of objects; nil when Limit==0 (count-only)
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

func (r CreateObjectRequest) String() string {
	return types.Stringify(r)
}

func (r ListObjectsRequest) String() string {
	return types.Stringify(r)
}

func (r GetObjectRequest) String() string {
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
