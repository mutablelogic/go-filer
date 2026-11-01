package fs

import (
	"io/fs"
	"mime"
	"path"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// objectFromInfo returns the object of the named file, which is a path in the
// filesystem, which is returned as an object path. A package is a file, of
// its type, whose size isn't known, as it's the size of the files in it, and
// a directory has neither a size nor a modification time. The content type
// is set as given: pass "" where it isn't needed, such as for a delete
// response.
func (self *FS) objectFromInfo(name string, info fs.FileInfo, contentType string) *schema.Object {
	obj := &schema.Object{ObjectKey: schema.ObjectKey{Path: objectPath(name)}}
	switch {
	case info.IsDir() && self.isPackage(path.Base(name)):
		obj.ModTime = info.ModTime()
		obj.ContentType = contentType
	case info.IsDir():
		obj.IsDir = true
		obj.ContentType = schema.DirectoryType
	default:
		obj.Size = types.Ptr(info.Size())
		obj.ModTime = info.ModTime()
		obj.ContentType = contentType
	}
	return obj
}

// objectType returns the content type of the named file, by its extension,
// or "" when it isn't known
func objectType(name string) string {
	return mime.TypeByExtension(path.Ext(name))
}

// objectPath converts a path in the filesystem to an object path, which
// starts with "/"
func objectPath(name string) string {
	if name == "." {
		return "/"
	}
	return "/" + name
}
