package fs

import (
	"context"
	"slices"
	"time"

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
	name := fsPath(req.Path)
	info, err := self.stat("stat", name)
	if err != nil {
		return nil, err
	}

	// Return the object, unless it hasn't changed
	obj := self.objectFromInfo(name, info, objectType(name))
	if notModified(obj, req) {
		return nil, filer.ErrNotModified.Withf("%q", obj.Path)
	}
	return obj, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// notModified returns true when the request's conditions say the object
// hasn't changed. As in HTTP, IfNoneMatch is used in place of
// IfModifiedSince when it's set, and times are compared to the second.
func notModified(obj *schema.Object, req schema.GetObjectRequest) bool {
	if len(req.IfNoneMatch) > 0 {
		return slices.Contains(req.IfNoneMatch, "*") || (obj.ETag != nil && slices.Contains(req.IfNoneMatch, *obj.ETag))
	} else if !req.IfModifiedSince.IsZero() && !obj.ModTime.IsZero() {
		return !obj.ModTime.Truncate(time.Second).After(req.IfModifiedSince.Truncate(time.Second))
	}
	return false
}
