// Package schema is the types of the objects in a backend, which are the
// types of the filer's objects
package schema

import (
	"context"
	"io"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// DirectoryType is the type of a directory, as freedesktop.org's shared MIME
// info has it, which has no content
const DirectoryType = "inode/directory"

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ObjectKey represents the unique identifier of an object, which consists of a volume and a path.
type ObjectKey struct {
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

// ListObjectsRequest lists the objects beneath a path in a volume
type ListObjectsRequest struct {
	ObjectKey
	Recursive  bool   `json:"recursive,omitempty"`
	StartAfter string `json:"start_after,omitempty" help:"Return objects after this path"`
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
