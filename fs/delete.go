package fs

import (
	"context"
	"io/fs"
	"path"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Delete an object in the filesystem, and return the deleted objects. A
// directory is only deleted when the request is recursive, in which case
// every object beneath it, including hidden files, is deleted too, and a
// package is deleted as a file, with the files in it. Any parent directories
// left empty are removed. The root of the volume cannot be deleted.
func (self *FS) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	if self.readonly {
		return nil, filer.ErrForbidden.Withf("Volume %q is read-only", self.name)
	} else if err := self.checkRequest(ctx); err != nil {
		return nil, err
	}

	// The root of the volume cannot be deleted
	name := fsPath(req.Path)
	if name == "." {
		return nil, filer.ErrBadParameter.Withf("Cannot delete the root of volume %q", self.name)
	}

	// Get the writable filesystem
	wfs, ok := self.fs.(filer.WriterFS)
	if !ok {
		return nil, filer.ErrForbidden.Withf("Volume %q is read-only", self.name)
	}

	// Check the path can be deleted, before looking it up
	if !isWritablePath(name) {
		return nil, &fs.PathError{Op: "delete", Path: req.Path, Err: fs.ErrInvalid}
	}

	// Get the object, or the objects beneath the directory, which will be
	// deleted. Content types aren't set: the objects no longer exist, and
	// nothing in a delete response needs them.
	info, err := self.stat("delete", name)
	if err != nil {
		return nil, err
	}
	objects := []*schema.Object{}
	if info.IsDir() && !self.isPackage(path.Base(name)) {
		if !req.Recursive {
			return nil, filer.ErrConflict.Withf("Cannot delete directory %q unless recursive", req.Path)
		} else if objects, err = self.filesBeneath(wfs, name); err != nil {
			return nil, err
		}
	} else {
		objects = append(objects, self.objectFromInfo(name, info, ""))
	}

	// Delete the object or directory
	if err := wfs.Delete(name); err != nil {
		return nil, err
	}

	// Return the deleted objects
	return &schema.DeleteObjectsResponse{Body: objects}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// filesBeneath returns the objects beneath a directory, including hidden
// files. Directories, and files which are not objects, are not returned,
// although they are deleted with the directory. A package is returned as
// one object, as it's listed, without the files in it.
func (self *FS) filesBeneath(fsys fs.FS, dir string) ([]*schema.Object, error) {
	objects := []*schema.Object{}
	err := fs.WalkDir(fsys, dir, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		} else if name != dir && entry.IsDir() && self.isPackage(entry.Name()) {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			objects = append(objects, self.objectFromInfo(name, info, ""))
			return fs.SkipDir
		} else if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		objects = append(objects, self.objectFromInfo(name, info, ""))
		return nil
	})
	return objects, err
}
