package indexer_test

import (
	"context"
	"io"
	"io/fs"
	"net/url"
	"slices"
	"strings"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

// bucket is a backend of keys, as a bucket is, where a folder is the prefix
// of a key, so a file, a, can be beside a folder, a/. A path of a folder
// ends with "/", and a path without it is a file, when there's one, or a
// folder. It can't be watched.
type bucket struct {
	mu   sync.Mutex
	keys map[string]bool
}

var _ gofiler.Filer = (*bucket)(nil)

func newBucket(keys ...string) *bucket {
	b := &bucket{keys: make(map[string]bool)}
	for _, key := range keys {
		b.keys[key] = true
	}
	return b
}

func (b *bucket) URL() *url.URL { return &url.URL{Scheme: "bucket", Host: "media"} }

func (b *bucket) Clone(context.Context, url.Values) (gofiler.Filer, error) { return b, nil }

func (b *bucket) WatchObjects(context.Context, chan<- schema.Event) error {
	return gofiler.ErrNotImplemented.With("a bucket isn't watched")
}

func (b *bucket) ReadObject(context.Context, schema.GetObjectRequest) (gofiler.ReadObjectFunc, *schema.Object, error) {
	return nil, nil, gofiler.ErrNotImplemented.With("not read")
}

// isFolder returns true when a prefix, such as a/, has keys, which must be
// called with the bucket locked
func (b *bucket) isFolder(prefix string) bool {
	if prefix == "" {
		return true
	}
	for key := range b.keys {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func (b *bucket) GetObject(_ context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := strings.TrimPrefix(req.Path, "/")
	switch {
	case strings.HasSuffix(req.Path, "/") || key == "":
		if b.isFolder(key) {
			return &schema.Object{ObjectKey: schema.ObjectKey{Path: "/" + strings.TrimSuffix(key, "/")}, ObjectAttr: schema.ObjectAttr{IsDir: true}}, nil
		}
	case b.keys[key]:
		return &schema.Object{ObjectKey: schema.ObjectKey{Path: req.Path}}, nil
	case b.isFolder(key + "/"):
		return &schema.Object{ObjectKey: schema.ObjectKey{Path: req.Path}, ObjectAttr: schema.ObjectAttr{IsDir: true}}, nil
	}
	return nil, &fs.PathError{Op: "get", Path: req.Path, Err: fs.ErrNotExist}
}

func (b *bucket) ListObjects(_ context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	prefix := strings.TrimPrefix(req.Path, "/")
	if !b.isFolder(prefix) {
		return nil, &fs.PathError{Op: "list", Path: req.Path, Err: fs.ErrNotExist}
	}
	seen := make(map[string]*schema.Object)
	for key := range b.keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		rest := strings.Split(strings.TrimPrefix(key, prefix), "/")
		for n := range rest {
			p := "/" + prefix + strings.Join(rest[:n+1], "/")
			if n == len(rest)-1 {
				seen[p] = &schema.Object{ObjectKey: schema.ObjectKey{Path: p}}
			} else {
				seen[p+"/"] = &schema.Object{ObjectKey: schema.ObjectKey{Path: p}, ObjectAttr: schema.ObjectAttr{IsDir: true}}
			}
			if !req.Recursive {
				break
			}
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	objects := make([]*schema.Object, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, seen[key])
	}
	return &objectIterator{objects: objects}, nil
}

func (b *bucket) CreateObject(_ context.Context, req schema.CreateObjectRequest) (*schema.Object, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.keys[strings.TrimPrefix(req.Path, "/")] = true
	return &schema.Object{ObjectKey: schema.ObjectKey{Path: req.Path}}, nil
}

func (b *bucket) DeleteObjects(_ context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := strings.TrimPrefix(req.Path, "/")
	if !b.keys[key] {
		return nil, &fs.PathError{Op: "delete", Path: req.Path, Err: fs.ErrNotExist}
	}
	delete(b.keys, key)
	return &schema.DeleteObjectsResponse{Body: []*schema.Object{{ObjectKey: schema.ObjectKey{Path: req.Path}}}}, nil
}

// objectIterator returns objects from a slice
type objectIterator struct {
	objects []*schema.Object
}

func (i *objectIterator) Next(context.Context) (*schema.Object, error) {
	if len(i.objects) == 0 {
		return nil, io.EOF
	}
	object := i.objects[0]
	i.objects = i.objects[1:]
	return object, nil
}

func (i *objectIterator) Close() error { return nil }
