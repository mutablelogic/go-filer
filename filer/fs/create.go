package fs

import (
	"context"
	"errors"
	"io/fs"
	"mime"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Create object in the filesystem, replacing any existing object at the same
// path. An object cannot be written over a directory, and when IfNotExists is
// set, cannot be written over an existing object, nor, when IfExists is set,
// created where there's none. A nil body creates an empty object. The object
// is replaced atomically, so if writing the body fails, or the context is
// cancelled, or the filesystem is closed, while it's written, any existing
// object is left unchanged. When ModTime is set, it is used as the
// modification time of the object. Metadata is not yet supported, and
// returns an error.
func (self *FS) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (_ *schema.Object, err error) {
	ctx, end := self.span(ctx, "CreateObject", req)
	defer func() { end(err) }()
	defer func() { err = objectError("create", req.Path, err) }()

	if self.readonly {
		return nil, filer.ErrForbidden.Withf("Volume %q is read-only", self.name)
	} else if err := self.checkRequest(ctx); err != nil {
		return nil, err
	}

	// TODO: Store metadata. Until then, reject it rather than silently
	// discarding it. The content type of an object is from its extension, so
	// one which is another type is rejected, as it wouldn't be returned.
	if len(req.Meta) > 0 {
		return nil, filer.ErrNotImplemented.With("Object metadata is not supported")
	} else if !sameType(req.ContentType, objectType(fsPath(req.Path))) {
		return nil, filer.ErrNotImplemented.Withf("Content type %q isn't supported, as the type of %q is from its extension", req.ContentType, req.Path)
	}

	// Get the writable filesystem
	wfs, ok := self.fs.(filer.WriterFS)
	if !ok {
		return nil, filer.ErrForbidden.Withf("Volume %q is read-only", self.name)
	}

	// The root of the volume is a directory, and cannot be written, nor can
	// a path which isn't valid, and a file isn't written where it would be
	// skipped, as it wouldn't be listed, such as a hidden file
	name := fsPath(req.Path)
	if name == "." || !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "create", Path: req.Path, Err: fs.ErrInvalid}
	} else if self.skipPath(name) {
		return nil, filer.ErrUnprocessableContent.Withf("%q is skipped, as its name isn't stored", req.Path)
	} else if self.inPackage(name) {
		return nil, filer.ErrUnprocessableContent.Withf("%q is in a package, whose files aren't listed", req.Path)
	}

	// Check for an existing directory or object at the path, which fails
	// early before reading the body
	if req.IfNotExists && req.IfExists {
		return nil, filer.ErrBadParameter.With("IfNotExists and IfExists can't both be set")
	} else if info, err := wfs.Stat(name); err == nil {
		if info.IsDir() {
			return nil, filer.ErrConflict.Withf("Cannot write object over directory: %q", req.Path)
		} else if !isObject(info.Mode()) {
			return nil, filer.ErrConflict.Withf("Cannot write object over a file which is not an object: %q", req.Path)
		} else if req.IfNotExists {
			return nil, filer.ErrConflict.Withf("Object already exists: %q", req.Path)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	} else if req.IfExists {
		return nil, filer.ErrPreconditionFailed.Withf("Object doesn't exist: %q", req.Path)
	}

	// Write the object, from a body which stops being read once the context
	// is cancelled, or the filesystem is closed. The checks above are
	// repeated when the object is moved into place, in case another object
	// was created, or deleted, at the same path in the meantime
	mode := filer.CreateOrReplace
	if req.IfNotExists {
		mode = filer.CreateOnly
	} else if req.IfExists {
		mode = filer.ReplaceOnly
	}
	body := req.Body
	if body != nil {
		body = &reader{ctx: ctx, fs: self, r: body}
	}
	info, err := wfs.Create(name, body, mode, req.ModTime)
	if errors.Is(err, fs.ErrExist) {
		return nil, filer.ErrConflict.Withf("Object already exists: %q", req.Path)
	} else if req.IfExists && errors.Is(err, fs.ErrNotExist) {
		return nil, filer.ErrPreconditionFailed.Withf("Object doesn't exist: %q", req.Path)
	} else if err != nil {
		return nil, err
	}

	// Return the object. The content type is from the extension, as the body
	// has been written, so sniffing it would mean reading the file again.
	return self.objectFromInfo(name, info, objectType(name)), nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// sameType returns true when the content type of a request is empty, or the
// same media type as the type of the object, ignoring parameters, such as
// the charset, and case
func sameType(contentType, objectType string) bool {
	if contentType == "" {
		return true
	}
	want, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	got, _, err := mime.ParseMediaType(objectType)
	return err == nil && got == want
}
