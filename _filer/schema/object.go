package schema

import (
	"encoding/json"
	"io"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// CONSTANTS

const (
	// MaxListLimit is the maximum number of objects that can be returned in a
	// single ListObjects call. Clients must paginate using Offset for larger sets.
	MaxListLimit = 1000

	// MaxUploadFiles is the maximum number of files accepted in a single multipart upload request.
	MaxUploadFiles = 1000
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
	Path      string `json:"path,omitempty"`      // optional path prefix within the backend
	Recursive bool   `json:"recursive,omitempty"` // if true, list all objects recursively; if false, list only immediate children
	Offset    int    `json:"offset,omitempty"`    // number of objects to skip before returning results (0-based)
	Limit     int    `json:"limit,omitempty"`     // max objects to return; 0 means count-only (Body will be nil)
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

////////////////////////////////////////////////////////////////////////////////
// READER

func (o *Object) Scan(row pg.Row) error {
	var meta []byte

	if err := row.Scan(
		&o.Name,
		&o.Path,
		&o.Size,
		&o.ModTime,
		&o.ContentType,
		&o.ETag,
		&meta,
	); err != nil {
		return err
	}

	if len(meta) == 0 {
		o.Meta = nil
		return nil
	}

	var objectMeta ObjectMeta
	if err := json.Unmarshal(meta, &objectMeta); err != nil {
		return err
	}
	o.Meta = objectMeta

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (k ObjectKey) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if k.Name == "" {
		return "", httpresponse.ErrBadRequest.With("missing object name")
	}
	if k.Path == "" {
		return "", httpresponse.ErrBadRequest.With("missing object path")
	}

	bind.Set("name", k.Name)
	bind.Set("path", k.Path)

	switch op {
	case pg.Get:
		return bind.Query("filer.object_get"), nil
	case pg.Delete:
		return bind.Query("filer.object_delete"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported ObjectKey operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (o Object) Insert(bind *pg.Bind) (string, error) {
	if o.Name == "" {
		return "", gofiler.ErrBadParameter.With("missing object name")
	}
	if o.Path == "" {
		return "", gofiler.ErrBadParameter.With("missing object path")
	}

	bind.Set("name", o.Name)
	bind.Set("path", o.Path)
	bind.Set("size", o.Size)

	if o.ModTime.IsZero() {
		bind.Set("modified_at", nil)
	} else {
		bind.Set("modified_at", o.ModTime.UTC())
	}

	bind.Set("type", o.ContentType)
	bind.Set("etag", o.ETag)

	meta := o.Meta
	if meta == nil {
		meta = ObjectMeta{}
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	bind.Set("meta", string(data))

	return bind.Query("filer.object_insert"), nil
}

func (o Object) Update(bind *pg.Bind) error {
	return gofiler.ErrInternalServerError.With("object update not implemented")
}
