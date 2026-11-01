package filer

import (
	"io"
	"io/fs"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// CreateMode is whether a file is created, replaced, or either
type CreateMode int

const (
	CreateOrReplace CreateMode = iota // a file is created, or replaces one which exists
	CreateOnly                        // a file which exists isn't replaced
	ReplaceOnly                       // a file which doesn't exist isn't created
)

// Implement a filesystem which also allows for creating and deleting files
type WriterFS interface {
	fs.StatFS

	// Method to create a file in the filesystem from the body, which also
	// creates any necessary parent directories. The file is replaced
	// atomically: readers see either the previous file or the complete new
	// one, and if writing fails the previous file is left unchanged. With
	// CreateOnly, an existing file is not replaced, and an error wrapping
	// fs.ErrExist is returned. With ReplaceOnly, a file which doesn't exist
	// is not created, and an error wrapping fs.ErrNotExist is returned. When
	// modtime is not zero, it is set as the modification time of the file.
	// Returns the information for the new file.
	Create(path string, body io.Reader, mode CreateMode, modtime time.Time) (fs.FileInfo, error)

	// Method to delete a file, or a directory and everything beneath it,
	// which also removes any empty parent directories. The root directory
	// cannot be deleted.
	Delete(path string) error

	// Release resources held by the filesystem
	io.Closer
}
