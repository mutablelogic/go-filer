package fs

import (
	"context"
	"io/fs"
	"path"
	"strings"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Get object metadata from the filesystem. Returns ErrNotModified when the
// request's conditions say the object hasn't changed: when its ETag is one
// of IfNoneMatch, or "*" is, or, when IfNoneMatch is empty, when it hasn't
// been modified since IfModifiedSince, to the second. A directory, which has
// no modification time, is always returned.
func (self *FS) GetObject(ctx context.Context, req schema.GetObjectRequest) (_ *schema.Object, err error) {
	ctx, end := self.span(ctx, "GetObject", req)
	defer func() { end(err) }()

	if err := self.checkRequest(ctx); err != nil {
		return nil, err
	}

	// Get the file information
	name, info, err := self.statPath("stat", req.Path)
	if err != nil {
		return nil, err
	}

	// Return the object, unless it hasn't changed
	obj := self.objectFromInfo(name, info, objectType(name))
	if obj.NotModified(req) {
		return nil, filer.ErrNotModified.Withf("%q", obj.Path)
	}
	return obj, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// statPath returns the name and information of the object at the path, for
// the operation, where a path which ends with "/" is a directory, which
// doesn't exist when it's a file, or a package, which is one
func (self *FS) statPath(op, p string) (string, fs.FileInfo, error) {
	trimmed := strings.TrimSuffix(p, "/")
	if trimmed == "" || strings.HasSuffix(trimmed, "/") {
		trimmed = p // a path such as "//" isn't valid
	}
	name := fsPath(trimmed)
	info, err := self.stat(op, name)
	if err != nil {
		return "", nil, err
	} else if name != "." && strings.HasSuffix(p, "/") && !(info.IsDir() && !self.isPackage(path.Base(name))) {
		return "", nil, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
	}
	return name, info, nil
}
