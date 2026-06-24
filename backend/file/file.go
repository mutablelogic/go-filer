package file

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"os"
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
	path string
	fs   fs.FS
}

var _ backend.Backend = (*FileBackend)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(url *url.URL) (*FileBackend, error) {
	self := new(FileBackend)

	name, err := Validate(url)
	if err != nil {
		return nil, err
	} else if fs, err := fs.Sub(os.DirFS(url.Path), "."); err != nil {
		return nil, err
	} else {
		self.name = name
		self.path = url.Path
		self.fs = fs
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
	url.Path = self.path
	return url
}

// Create object in the backend
func (FileBackend) CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error) {
	return nil, gofiler.ErrNotImplemented
}

// Get object metadata from the backend
func (self *FileBackend) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	name := strings.TrimPrefix(path.Clean("/"+strings.TrimSpace(req.Path)), "/")
	if name == "" {
		name = "."
	}
	if name != "." && !fs.ValidPath(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid object path %q", req.Path)
	}

	info, err := fs.Stat(self.fs, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, gofiler.ErrNotFound.Withf("object not found: %q", req.Path)
		}
		return nil, err
	}

	resultPath := "/"
	if name != "." {
		resultPath += name
	}

	contentType := types.ContentTypeBinary
	if !info.IsDir() {
		f, err := self.fs.Open(name)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		contentType = mime.Type(f)
	}

	return &schema.Object{
		ObjectKey: schema.ObjectKey{
			Volume: self.name,
			Path:   resultPath,
		},
		ObjectMeta: schema.ObjectMeta{
			ContentType: contentType,
		},
		ObjectAttr: schema.ObjectAttr{
			Size:    info.Size(),
			ModTime: info.ModTime(),
			IsDir:   info.IsDir(),
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

// List objects in the backend
func (self FileBackend) ListObjects(ctx context.Context, req schema.ObjectListRequest) (*schema.ObjectList, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Volume != self.name {
		return nil, gofiler.ErrNotFound.Withf("volume not found: %q", req.Volume)
	}

	rawPath := "/"
	if req.Path != nil {
		rawPath = strings.TrimSpace(*req.Path)
	}

	name := strings.TrimPrefix(path.Clean("/"+rawPath), "/")
	if name == "" {
		name = "."
	}
	if name != "." && !fs.ValidPath(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid object path %q", rawPath)
	}

	baseInfo, err := fs.Stat(self.fs, name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, gofiler.ErrNotFound.Withf("object path not found: %q", rawPath)
		}
		return nil, err
	}

	// Depth of the starting path used to enforce non-recursive listing.
	baseDepth := 0
	if name != "." {
		baseDepth = strings.Count(name, "/") + 1
	}

	var count int
	var body []*schema.Object
	limit := req.Limit
	collect := limit == nil || *limit > 0

	err = fs.WalkDir(self.fs, name, func(filename string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		// Skip hidden files and directories anywhere in the tree.
		entry := path.Base(filename)
		if entry != "." && strings.HasPrefix(entry, ".") {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		// Skip the root directory itself when listing under a directory.
		if filename == name && d.IsDir() {
			return nil
		}

		if !req.Recursive && baseInfo.IsDir() {
			depth := strings.Count(filename, "/") + 1
			if depth > baseDepth+1 {
				if d.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			// Skip non-regular files (e.g. symlinks, devices)
			return nil
		}

		count++
		if count <= int(req.Offset) || !collect {
			return nil
		}
		if limit != nil && uint64(len(body)) >= *limit {
			return nil
		}

		objectPath := "/"
		if filename != "." {
			objectPath += filename
		}

		contentType := types.ContentTypeBinary
		if !info.IsDir() {
			contentType = mime.TypeByExtension(path.Ext(filename))
		}

		body = append(body, &schema.Object{
			ObjectKey: schema.ObjectKey{
				Volume: self.name,
				Path:   objectPath,
			},
			ObjectMeta: schema.ObjectMeta{
				ContentType: contentType,
			},
			ObjectAttr: schema.ObjectAttr{
				Size:    info.Size(),
				ModTime: info.ModTime(),
				IsDir:   info.IsDir(),
			},
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &schema.ObjectList{
		ObjectListRequest: req,
		Count:             count,
		Body:              body,
	}, nil
}

// Delete objects in the backend (single object or prefix)
func (FileBackend) DeleteObjects(context.Context, schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	return nil, gofiler.ErrNotImplemented
}
