// Package schema is the types of the objects in a backend, which are the
// types of the filer's objects
package schema

import (
	"context"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	// Packages
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// DirectoryType is the type of a directory, as freedesktop.org's shared MIME
// info has it, which has no content
const DirectoryType = "inode/directory"

// ObjectListLimit is the most objects in a page of a listing, which is the
// number when the request doesn't set one
const ObjectListLimit = 100

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ObjectKey represents the unique identifier of an object, which consists of a volume and a path.
// The name of the volume isn't used by a backend, which is a single volume,
// but identifies the volume's backend in the registry.
type ObjectKey struct {
	Name string `json:"name,omitempty" help:"Volume name"`
	Path string `json:"path,omitempty" arg:"" required:"" help:"Object path"`
}

// ObjectAttr represents the attributes of an object, which are immutable and cannot be updated.
type ObjectAttr struct {
	Size    *int64    `json:"size,omitempty"`
	ETag    *string   `json:"etag,omitempty"`
	ModTime time.Time `json:"mtime,omitzero"`
	IsDir   bool      `json:"dir,omitempty"`
}

// ObjectMeta represents the metadata of an object, which can be updated.
type ObjectMeta struct {
	ContentType string `json:"type,omitempty"`
	Meta        []Meta `json:"meta,omitempty"`
}

// Object represents a file or directory in a volume, including its key, metadata, and attributes.
type Object struct {
	ObjectKey
	ObjectMeta
	ObjectAttr
}

// Create an object in the filesystem
type CreateObjectRequest struct {
	ObjectKey
	Body        io.Reader `json:"-"`
	IfNotExists bool      // if true, fail with ErrConflict when the object already exists
	IfExists    bool      // if true, fail with ErrPreconditionFailed when the object doesn't exist
	ModTime     time.Time `json:"last-modified,omitzero"`
	ObjectMeta
}

// Return an object
type GetObjectRequest struct {
	ObjectKey
	IfModifiedSince time.Time `json:"-"` // if non-zero, only return the object if it has been modified since this time
	IfNoneMatch     []string  `json:"-"` // if non-empty, only return the object if its ETag doesn't match any of these
}

// ListObjectsRequest lists the objects beneath a path in a volume, which is
// a directory, or an object, when only it's listed. A path which ends with
// "/" is a directory, which is listed, even when an object has its name, as
// it can in a bucket, and which doesn't exist when it's an object.
type ListObjectsRequest struct {
	ObjectKey
	Recursive  bool   `json:"recursive,omitempty"`
	StartAfter string `json:"start_after,omitempty" help:"Return objects after this path"`
}

// ObjectListRequest lists the objects beneath a path in a volume, a page at
// a time. The next page starts after the key of the last object of the
// previous page, which is the list's Next.
type ObjectListRequest struct {
	ListObjectsRequest
	Limit *uint64 `json:"limit,omitempty" help:"Maximum number of objects to return, which defaults to and is capped at 100"`
}

// ObjectList is a page of the objects beneath a path in a volume, in the
// same order as S3, by their keys. When there are more, Next is the key of
// the last object, which the next page starts after.
type ObjectList struct {
	ObjectListRequest
	Next string    `json:"next,omitempty" help:"Start after this key for the next page"`
	Body []*Object `json:"body"`
}

// ObjectUploadResponse is the files which were uploaded into a directory, in
// the order they were, and those which were skipped, and why
type ObjectUploadResponse struct {
	Body    []*Object       `json:"body" help:"Files which were written"`
	Skipped []ObjectSkipped `json:"skipped,omitempty" help:"Files which weren't written, and why"`
}

// ObjectSkipped is a file which wasn't uploaded, by its path in the volume,
// and why, such as it exists, and the upload doesn't overwrite it, or its
// name isn't stored, such as a hidden file
type ObjectSkipped struct {
	Path   string `json:"path" help:"Path of the file in the volume"`
	Exists bool   `json:"exists,omitempty" help:"The file exists, and the upload doesn't overwrite it"`
	Reason string `json:"reason" help:"Why the file was skipped"`
}

// Iterates over the objects returned by ListObjects.
// Caller must close the iterator when done.
type ListObjectsIterator interface {
	io.Closer

	// Next returns the next object, or io.EOF when there are no more objects
	Next(context.Context) (*Object, error)
}

// DeleteObjectsRequest deletes an object. A directory is only deleted when
// Recursive is true, in which case every object beneath it is deleted too.
type DeleteObjectsRequest struct {
	ObjectKey
	Recursive bool `json:"recursive,omitempty"`
}

// DeleteObjectsResponse is the objects which were deleted, which is the
// object, or the objects beneath a directory, but not directories
type DeleteObjectsResponse struct {
	Body []*Object `json:"body" readonly:"" help:"Objects deleted, not including directories"`
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Query returns the query of the request for a page of the objects in a
// volume, whose name is the path of the request
func (r ObjectListRequest) Query() url.Values {
	values := url.Values{}
	if r.Path != "" {
		values.Set("path", r.Path)
	}
	if r.Recursive {
		values.Set("recursive", "true")
	}
	if r.StartAfter != "" {
		values.Set("start_after", r.StartAfter)
	}
	if r.Limit != nil {
		values.Set("limit", strconv.FormatUint(*r.Limit, 10))
	}
	return values
}

// Key returns the key of the object, which is its path, or for a directory,
// its path followed by "/", which is the order objects are listed in
// NotModified returns true when the request's conditions say the object
// hasn't changed: when its ETag is one of IfNoneMatch, or "*" is, or, when
// IfNoneMatch is empty, when it hasn't been modified since IfModifiedSince.
// As in HTTP, IfNoneMatch is used in place of IfModifiedSince when it's set,
// and times are compared to the second. An object without a modification
// time, such as a directory, has always been modified.
func (o Object) NotModified(req GetObjectRequest) bool {
	if len(req.IfNoneMatch) > 0 {
		return slices.Contains(req.IfNoneMatch, "*") || (o.ETag != nil && slices.Contains(req.IfNoneMatch, *o.ETag))
	} else if !req.IfModifiedSince.IsZero() && !o.ModTime.IsZero() {
		return !o.ModTime.Truncate(time.Second).After(req.IfModifiedSince.Truncate(time.Second))
	}
	return false
}

func (o Object) Key() string {
	if o.IsDir && !strings.HasSuffix(o.Path, "/") {
		return o.Path + "/"
	}
	return o.Path
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (o Object) String() string {
	return types.Stringify(o)
}

func (l ObjectList) String() string {
	return types.Stringify(l)
}

////////////////////////////////////////////////////////////////////////////////
// TABLE

func (o Object) Header() []string {
	return []string{"Key", "Type", "Size", "Modified"}
}

func (o Object) Width(col int) int {
	return 0
}

func (o Object) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return o.Key()
	case 1:
		if o.IsDir {
			return ""
		}
		return o.ContentType
	case 2:
		if o.Size == nil {
			return ""
		}
		return FormatSize(*o.Size)
	case 3:
		if o.ModTime.IsZero() {
			return ""
		}
		return o.ModTime.Local().Format(time.DateTime)
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// FormatSize returns a size in bytes, or in KiB, MiB, GiB or TiB, with one
// decimal place, when it's larger
func FormatSize(size int64) string {
	const unit = 1024
	if size < unit {
		return strconv.FormatInt(size, 10) + " B"
	}
	value, prefix := float64(size)/unit, 0
	for value >= unit && prefix < 3 {
		value, prefix = value/unit, prefix+1
	}
	return strconv.FormatFloat(value, 'f', 1, 64) + " " + string("KMGT"[prefix]) + "iB"
}
