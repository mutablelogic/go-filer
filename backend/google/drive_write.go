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
// name which is hidden, which isn't listed, or which starts, or ends, with
// a space, which a name isn't listed with, nor of a name which is
// another's in its folder, ignoring case, which would rename both, and, of
// IfNotExists, over a file, nor, of IfExists, where there's none. Its
// metadata isn't written, so far.
//
// A new file's folders, and the file, without content, are created first,
// with its name, which only one create does at a time, so two creates of a
// path don't both create it, and its content is then uploaded, as a new
// revision of it, at the same time as others'. A folder which is created
// stays, when the upload fails, but a new file is binned.
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
		if name != strings.TrimSpace(name) {
			return nil, gofiler.ErrBadParameter.Withf("%q has a name which starts, or ends, with a space", req.Path)
		} else if strings.HasPrefix(name, ".") {
			return nil, gofiler.ErrUnprocessableContent.Withf("%q is skipped, as its name is hidden", req.Path)
		}
	}

	// Its type is the request's, or its extension's, or, of a new file,
	// binary, or else a file's is kept
	write := httpclient.DriveWrite{ModifiedTime: req.ModTime.UTC(), MimeType: req.ContentType}
	if write.MimeType == "" {
		write.MimeType = mime.TypeByExtension(path.Ext(key))
	}

	// The file, which is there, or is created, with its folders
	file, created, err := self.reserve(ctx, req, key, write.MimeType)
	if err != nil {
		return nil, err
	}

	// The content, as it's read, which is empty, without a body
	body := req.Body
	if body == nil {
		body = strings.NewReader("")
	}
	uploaded, err := self.client.Upload(ctx, file.Id, write, body, self.upload)
	if err != nil {
		if created {
			// The new file, without its content, isn't kept
			if trashed, terr := self.client.Trash(context.WithoutCancel(ctx), file.Id); terr == nil {
				self.wrote(trashed)
			}
		}
		return nil, self.apiError("create", req.Path, err)
	}
	self.wrote(uploaded)
	return self.object(&driveTree{files: map[string]*httpclient.DriveFile{key: uploaded}}, key), nil
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

// reserve returns the file at the path, which is written over, or, when
// there isn't one, creates it, without content, and its folders, when they
// aren't there, of names which aren't another's, ignoring case, and returns
// true, which one create does at a time, so two don't both create it
func (self *DriveFiler) reserve(ctx context.Context, req schema.CreateObjectRequest, key, mimeType string) (*httpclient.DriveFile, bool, error) {
	self.naming.Lock()
	defer self.naming.Unlock()
	tree, err := self.files(ctx, "create", req.Path)
	if err != nil {
		return nil, false, err
	}
	file, exists := tree.files[key]
	switch {
	case exists && file.MimeType == httpclient.DriveFolderType:
		return nil, false, gofiler.ErrConflict.Withf("Cannot write object over directory: %q", req.Path)
	case exists && isExport(file):
		return nil, false, gofiler.ErrForbidden.Withf("%q is a Google Doc, Sheet or Slides, whose content is an export, which isn't written", req.Path)
	case exists && req.IfNotExists:
		return nil, false, gofiler.ErrConflict.Withf("Object already exists: %q", req.Path)
	case exists:
		return file, false, nil
	case req.IfExists:
		return nil, false, gofiler.ErrPreconditionFailed.Withf("Object doesn't exist: %q", req.Path)
	}

	// Its folders, which are created, when they aren't there
	var folders []*httpclient.DriveFile
	defer func() {
		if len(folders) > 0 {
			self.wrote(folders...)
		}
	}()
	parent, dir := tree.root, "/"
	for _, name := range strings.Split(strings.TrimPrefix(path.Dir(key), "/"), "/") {
		if name == "" {
			continue
		}
		p := path.Join(dir, name)
		if folder, exists := tree.files[p]; exists && folder.MimeType != httpclient.DriveFolderType {
			return nil, false, gofiler.ErrConflict.Withf("%q is a file, not a directory", p)
		} else if exists {
			parent, dir = folder.Id, p
			continue
		} else if taken := tree.taken(dir, name); taken != "" {
			return nil, false, gofiler.ErrConflict.Withf("%q is the name of %q, ignoring case", p, taken)
		}
		folder, err := self.client.CreateFile(ctx, httpclient.DriveWrite{Name: name, Parents: []string{parent}, MimeType: httpclient.DriveFolderType})
		if err != nil {
			return nil, false, self.apiError("create", p, err)
		}
		folders = append(folders, folder)
		parent, dir = folder.Id, p
		tree = &driveTree{root: tree.root, files: map[string]*httpclient.DriveFile{}, children: map[string][]string{}}
	}

	// The file, without content
	if taken := tree.taken(dir, path.Base(key)); taken != "" {
		return nil, false, gofiler.ErrConflict.Withf("%q is the name of %q, ignoring case", key, taken)
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	file, err = self.client.CreateFile(ctx, httpclient.DriveWrite{Name: path.Base(key), Parents: []string{parent}, MimeType: mimeType})
	if err != nil {
		return nil, false, self.apiError("create", req.Path, err)
	}
	folders = append(folders, file)
	return file, true, nil
}

// taken returns the path of a file, or folder, in a directory, whose name
// in Drive, or in its path, such as one with the start of its id, is the
// name, ignoring case, or empty, when there's none, as a name which is
// another's, ignoring case, has the start of its id, so a new file of it
// wouldn't be at its path, and would change the other's
func (tree *driveTree) taken(dir, name string) string {
	for _, p := range tree.children[dir] {
		if file := tree.files[p]; file != nil && (strings.EqualFold(driveName(file), name) || strings.EqualFold(path.Base(p), name)) {
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
