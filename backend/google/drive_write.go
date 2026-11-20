package google

import (
	"context"
	"io/fs"
	"mime"
	"path"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// CreateObject uploads a file, as it's read, of its path, which is a new
// revision of the file at the path, when there's one, keeping its id, link,
// and who it's shared with, or a new file, in the folders of its path,
// which are created when they aren't there. Its type is the request's, or
// its extension's, and its modification time the request's, when it's set.
// A file isn't written over a folder, nor over a Google Doc, Sheet or
// Slides, which is ErrForbidden, as their content is an export, nor of a
// name which is hidden, which isn't listed, nor of a name which is another's
// in its folder, ignoring case, which would rename both, and, of
// IfNotExists, over a file, nor, of IfExists, where there's none. Its
// metadata isn't written, so far. A folder which is created stays, when the
// upload fails.
func (self *DriveFiler) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	if self.readonly {
		return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
	} else if req.IfNotExists && req.IfExists {
		return nil, gofiler.ErrBadParameter.With("IfNotExists and IfExists can't both be set")
	} else if len(req.Meta) > 0 {
		return nil, gofiler.ErrNotImplemented.With("Object metadata is not supported")
	}
	key := path.Clean("/" + req.Path)
	if key == "/" || strings.HasSuffix(req.Path, "/") {
		return nil, &fs.PathError{Op: "create", Path: req.Path, Err: fs.ErrInvalid}
	}
	for _, name := range strings.Split(strings.TrimPrefix(key, "/"), "/") {
		if strings.HasPrefix(strings.TrimSpace(name), ".") {
			return nil, gofiler.ErrUnprocessableContent.Withf("%q is skipped, as its name is hidden", req.Path)
		}
	}
	tree, err := self.files(ctx, "create", req.Path)
	if err != nil {
		return nil, err
	}

	// A file which is there is written over, of a new revision
	file, exists := tree.files[key]
	switch {
	case exists && file.MimeType == httpclient.DriveFolderType:
		return nil, gofiler.ErrConflict.Withf("Cannot write object over directory: %q", req.Path)
	case exists && isExport(file):
		return nil, gofiler.ErrForbidden.Withf("%q is a Google Doc, Sheet or Slides, whose content is an export, which isn't written", req.Path)
	case exists && req.IfNotExists:
		return nil, gofiler.ErrConflict.Withf("Object already exists: %q", req.Path)
	case !exists && req.IfExists:
		return nil, gofiler.ErrPreconditionFailed.Withf("Object doesn't exist: %q", req.Path)
	}

	// Of a new file, its folders, which are created, when they aren't there,
	// whose names, and its own, aren't another's, ignoring case
	var folders []*httpclient.DriveFile
	parent, write := tree.root, httpclient.DriveWrite{ModifiedTime: req.ModTime.UTC(), MimeType: req.ContentType}
	if write.MimeType == "" && !exists {
		if write.MimeType = mime.TypeByExtension(path.Ext(key)); write.MimeType == "" {
			write.MimeType = "application/octet-stream"
		}
	}
	if !exists {
		dir := "/"
		for _, name := range strings.Split(strings.TrimPrefix(path.Dir(key), "/"), "/") {
			if name == "" {
				continue
			}
			p := path.Join(dir, name)
			if folder, exists := tree.files[p]; exists && folder.MimeType != httpclient.DriveFolderType {
				return nil, gofiler.ErrConflict.Withf("%q is a file, not a directory", p)
			} else if exists {
				parent, dir = folder.Id, p
				continue
			} else if taken := tree.taken(dir, name); taken != "" {
				return nil, gofiler.ErrConflict.Withf("%q is the name of %q, ignoring case", p, taken)
			}
			folder, err := self.client.CreateFolder(ctx, name, parent)
			if err != nil {
				return nil, self.writeError("create", p, folders, err)
			}
			folders = append(folders, folder)
			parent, dir = folder.Id, p
			tree = &driveTree{root: tree.root, files: map[string]*httpclient.DriveFile{}, children: map[string][]string{}}
		}
		if taken := tree.taken(dir, path.Base(key)); taken != "" {
			return nil, gofiler.ErrConflict.Withf("%q is the name of %q, ignoring case", key, taken)
		}
		write.Name, write.Parents = path.Base(key), []string{parent}
	}

	// The content, as it's read, which is empty, without a body
	body := req.Body
	if body == nil {
		body = strings.NewReader("")
	}
	var id string
	if exists {
		id = file.Id
	}
	uploaded, err := self.client.Upload(ctx, id, write, body, self.upload)
	if err != nil {
		return nil, self.writeError("create", req.Path, folders, err)
	}
	self.wrote(append(folders, uploaded)...)
	return self.object(&driveTree{root: tree.root, files: map[string]*httpclient.DriveFile{key: uploaded}}, key), nil
}

// DeleteObjects moves a file, or, when it's recursive, a folder, with what's
// in it, to the bin, from which it can be restored, and returns the files
// which are deleted, but not the folders. The root can't be deleted, nor a
// folder, unless it's recursive.
func (self *DriveFiler) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	if self.readonly {
		return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
	}
	key := path.Clean("/" + req.Path)
	if key == "/" {
		return nil, gofiler.ErrBadParameter.Withf("Cannot delete the root of volume %q", self.name)
	}
	tree, err := self.files(ctx, "delete", req.Path)
	if err != nil {
		return nil, err
	}
	object, file, err := self.lookup(tree, "delete", req.Path)
	if err != nil {
		return nil, err
	}

	// The files which are deleted, of a folder, beneath it
	objects := []*schema.Object{}
	if object.IsDir {
		if !req.Recursive {
			return nil, gofiler.ErrConflict.Withf("Cannot delete directory %q unless recursive", req.Path)
		}
		var walk func(dir string)
		walk = func(dir string) {
			for _, p := range tree.children[dir] {
				if child := self.object(tree, p); child.IsDir {
					walk(p)
				} else {
					objects = append(objects, child)
				}
			}
		}
		walk(object.Path)
	} else {
		objects = append(objects, object)
	}
	trashed, err := self.client.Trash(ctx, file.Id)
	if err != nil {
		return nil, self.apiError("delete", req.Path, err)
	}
	self.wrote(trashed)
	return &schema.DeleteObjectsResponse{Body: objects}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// writeError returns the error of a write, after the folders it created,
// which stay, are applied, so they're listed
func (self *DriveFiler) writeError(op, p string, folders []*httpclient.DriveFile, err error) error {
	if len(folders) > 0 {
		self.wrote(folders...)
	}
	return self.apiError(op, p, err)
}

// taken returns the path of a file, or folder, in a directory, whose name
// in Drive is the name, ignoring case, or empty, when there's none, as a
// name which is another's, ignoring case, has the start of its id, so a new
// file of it wouldn't be at its path
func (tree *driveTree) taken(dir, name string) string {
	for _, p := range tree.children[dir] {
		if file := tree.files[p]; file != nil && strings.EqualFold(driveName(file), name) {
			return p
		}
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// isExport returns true of a Google Doc, Sheet or Slides, whose content is
// an export
func isExport(file *httpclient.DriveFile) bool {
	_, exists := driveExports[file.MimeType]
	return exists
}
