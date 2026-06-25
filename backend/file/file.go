package file

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"path"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	backend "github.com/mutablelogic/go-filer/backend"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	mime "github.com/mutablelogic/go-filer/metadata/mime"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type FileBackend struct {
	name string
	fs   WritableFS
}

type token struct {
	Offset uint64  // Offset of the next object to return
	Limit  *uint64 // Maximum number of objects to return for each iteration
}

var _ backend.Backend = (*FileBackend)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(url *url.URL) (*FileBackend, error) {
	self := new(FileBackend)

	name, err := Validate(url)
	if err != nil {
		return nil, err
	} else if wfs, err := newWritableFS(url.Path); err != nil {
		return nil, err
	} else {
		self.name = name
		self.fs = wfs
	}

	return self, nil
}

func (FileBackend) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Name returns the name of the backend
func (self *FileBackend) Name() string {
	return self.name
}

// URL returns the backend destination URL. The scheme, host (bucket/name),
// and path (prefix/directory) identify the storage location. Query
// parameters carry useful non-credential details: region, endpoint, anonymous.
func (self *FileBackend) URL() *url.URL {
	url := new(url.URL)
	url.Scheme = "file"
	url.Host = self.name
	url.Path = self.fs.Root()
	return url
}

// Create object in the backend
func (self *FileBackend) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// IfNotExists is true, we should fail if the object already exists
	_, info, err := self.statObject(req.ObjectKey)
	if req.IfNotExists && err == nil {
		return nil, gofiler.ErrConflict.Withf("object already exists: %q", req.Path)
	} else if err != nil && !errors.Is(err, gofiler.ErrNotFound) {
		return nil, err
	} else if info != nil && info.IsDir() {
		return nil, gofiler.ErrBadParameter.Withf("cannot create object at directory path: %q", req.Path)
	}

	// Create the object in the file system
	r, err := self.fs.Create(req.Path)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	// Copy the body to the file
	if req.Body != nil {
		if _, err := io.Copy(r, req.Body); err != nil {
			return nil, err
		}
	}

	// Return the object metadata
	return self.GetObject(ctx, schema.GetObjectRequest{ObjectKey: req.ObjectKey})
}

// Get object metadata from the backend
func (self *FileBackend) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	prefix, info, err := self.statObject(req.ObjectKey)
	if err != nil {
		return nil, err
	} else if info.IsDir() {
		return nil, gofiler.ErrBadParameter.Withf("path is a directory: %q", req.Path)
	}

	f, err := self.fs.Open(path.Join(prefix, info.Name()))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	contentType, meta, err := mime.Type(f)
	if err != nil {
		return nil, err
	}

	return &schema.Object{
		ObjectKey: schema.ObjectKey{
			Volume: self.name,
			Path:   path.Join(prefix, info.Name()),
		},
		ObjectMeta: schema.ObjectMeta{
			ContentType: contentType,
			Meta:        meta,
		},
		ObjectAttr: schema.ObjectAttr{
			Size:    info.Size(),
			ModTime: info.ModTime(),
		},
	}, nil
}

// Read object content from the backend. Caller must close the returned reader.
func (self *FileBackend) ReadObject(ctx context.Context, req schema.GetObjectRequest) (io.ReadCloser, *schema.Object, error) {
	// Get the object
	object, err := self.GetObject(ctx, req)
	if err != nil {
		return nil, nil, err
	} else if object.IsDir {
		return nil, nil, gofiler.ErrBadParameter.Withf("cannot read content of a directory: %q", req.Path)
	}

	// Open the file - caller is responsible for closing the reader
	f, err := self.fs.Open(strings.TrimPrefix(path.Clean("/"+strings.TrimSpace(req.Path)), "/"))
	if err != nil {
		return nil, nil, err
	}

	// Return the reader and object metadata
	return f, object, nil
}

// List objects or directories in the backend
func (self FileBackend) ListObjects(ctx context.Context, iterator *schema.ObjectListIterator) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tok, ok := iterator.Token.(*token)
	if tok == nil || !ok {
		tok = types.Ptr(token{Offset: 0, Limit: types.Ptr(uint64(schema.ObjectListLimit))})
		iterator.Token = tok
	}
	iterator.Body = make([]*schema.Object, 0, schema.ObjectListLimit)

	// Normalise the walk root the same way statObject does, so leading/trailing
	// slashes and dot segments are stripped before passing to fs.WalkDir.
	walkRoot := strings.TrimPrefix(path.Clean("/"+strings.TrimSpace(types.Value(iterator.Path))), "/")
	if walkRoot == "" {
		walkRoot = "."
	}

	// Reflect the normalised path back so callers see the canonical form.
	if walkRoot == "." {
		iterator.Path = nil
	} else {
		iterator.Path = types.Ptr(walkRoot)
	}

	// Ensure the path exists and is a directory
	if _, info, err := self.statObject(schema.ObjectKey{Path: walkRoot}); err != nil {
		return err
	} else if !info.IsDir() {
		return gofiler.ErrBadParameter.Withf("not a directory: %q", walkRoot)
	}

	errPageFull := errors.New("page full")

	n := uint64(0)
	appendEntry := func(entryPath string, isDir bool) error {
		defer func() { n++ }()
		if n < tok.Offset {
			return nil
		}
		if tok.Limit != nil && uint64(len(iterator.Body)) >= *tok.Limit {
			return errPageFull
		}
		if isDir {
			iterator.Body = append(iterator.Body, &schema.Object{
				ObjectKey:  schema.ObjectKey{Volume: self.name, Path: entryPath},
				ObjectMeta: schema.ObjectMeta{ContentType: schema.ContentTypeDirectory},
				ObjectAttr: schema.ObjectAttr{IsDir: true},
			})
		} else {
			obj, err := self.GetObject(ctx, schema.GetObjectRequest{
				ObjectKey: schema.ObjectKey{Volume: self.name, Path: entryPath},
			})
			if err != nil {
				return err
			}
			iterator.Body = append(iterator.Body, obj)
		}
		return nil
	}

	// Walk the directory tree and emit objects to the iterator
	err := fs.WalkDir(self.fs, walkRoot, func(entryPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		// Skip the walk root entry and hidden files/directories anywhere in the tree.
		if entryPath == walkRoot {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".") {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		// Emit directories when the caller is listing directories, or else emit files
		if d.IsDir() && types.Value(iterator.Type) == schema.ContentTypeDirectory {
			if err := appendEntry(entryPath, true); err != nil {
				return err
			}
		} else if d.Type().IsRegular() && types.Value(iterator.Type) != schema.ContentTypeDirectory {
			if err := appendEntry(entryPath, false); err != nil {
				return err
			}
		}

		// Descend into directories only when recursive; skip otherwise
		if d.IsDir() {
			if iterator.Recursive {
				return nil
			}
			return fs.SkipDir
		}

		return nil
	})

	if errors.Is(err, errPageFull) {
		tok.Offset += uint64(len(iterator.Body))
		return nil
	}
	if err != nil {
		return err
	}
	iterator.Token = nil
	return io.EOF
}

// Delete objects in the backend (single object or prefix)
func (self FileBackend) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entryPath, info, err := self.statObject(req.ObjectKey)
	if err != nil {
		return err
	}
	name := path.Join(entryPath, info.Name())
	if info.IsDir() {
		return self.fs.RemoveAll(name)
	}
	return self.fs.Remove(name)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (self *FileBackend) statObject(req schema.ObjectKey) (string, fs.FileInfo, error) {
	// Check the volume matches
	if req.Volume != "" && req.Volume != self.name {
		return "", nil, gofiler.ErrBadParameter.Withf("volume mismatch: %q != %q", req.Volume, self.name)
	}

	// Check the path is valid
	name := strings.TrimPrefix(path.Clean("/"+strings.TrimSpace(req.Path)), "/")
	if name == "" {
		name = "."
	}
	if name != "." && !fs.ValidPath(name) {
		return "", nil, gofiler.ErrBadParameter.Withf("invalid object path %q", req.Path)
	}

	// Stat the object
	info, err := fs.Stat(self.fs, name)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil, gofiler.ErrNotFound.Withf("object not found: %q", req.Path)
	} else if err != nil {
		return "", nil, err
	}

	// Return the file info
	return path.Dir(name), info, nil
}
