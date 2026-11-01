package fs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// memFS is a writable filesystem in memory, for testing
type memFS struct {
	sync.Mutex
	fstest.MapFS
}

var _ filer.WriterFS = (*memFS)(nil)

func (m *memFS) Open(name string) (fs.File, error) {
	m.Lock()
	defer m.Unlock()
	return m.MapFS.Open(name)
}

func (m *memFS) Stat(name string) (fs.FileInfo, error) {
	m.Lock()
	defer m.Unlock()
	return m.MapFS.Stat(name)
}

// Create reads the whole body before replacing the file, so a body which
// fails leaves the file unchanged
func (m *memFS) Create(name string, body io.Reader, mode filer.CreateMode, modtime time.Time) (fs.FileInfo, error) {
	var data []byte
	if body != nil {
		var err error
		if data, err = io.ReadAll(body); err != nil {
			return nil, err
		}
	}
	m.Lock()
	defer m.Unlock()
	_, exists := m.MapFS[name]
	if mode == filer.CreateOnly && exists {
		return nil, &fs.PathError{Op: "create", Path: name, Err: fs.ErrExist}
	} else if mode == filer.ReplaceOnly && !exists {
		return nil, &fs.PathError{Op: "create", Path: name, Err: fs.ErrNotExist}
	}
	if modtime.IsZero() {
		modtime = time.Now()
	}
	m.MapFS[name] = &fstest.MapFile{Data: data, ModTime: modtime}
	return m.MapFS.Stat(name)
}

func (m *memFS) Delete(name string) error {
	m.Lock()
	defer m.Unlock()
	delete(m.MapFS, name)
	return nil
}

func (m *memFS) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// TESTS

func Test_CreateObject(t *testing.T) {
	files := &memFS{MapFS: fstest.MapFS{
		"music":                        {Mode: fs.ModeDir},
		"old.txt":                      {Data: []byte("old")},
		"Disk.sparsebundle/bands/404d": {Data: []byte("d")},
	}}
	self, err := NewFS("test", files, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	create := func(req schema.CreateObjectRequest) (*schema.Object, error) {
		return self.CreateObject(ctx, req)
	}
	key := func(path string) schema.ObjectKey {
		return schema.ObjectKey{Path: path}
	}
	content := func(name string) string {
		if file, ok := files.MapFS[name]; ok {
			return string(file.Data)
		}
		return "<missing>"
	}

	// An object is created, with its modification time, and its type
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	obj, err := create(schema.CreateObjectRequest{ObjectKey: key("/music/song.mp3"), Body: strings.NewReader("abc"), ModTime: modtime})
	if err != nil {
		t.Fatal(err)
	} else if obj.Path != "/music/song.mp3" || *obj.Size != 3 || !obj.ModTime.Equal(modtime) || obj.ContentType != "audio/mpeg" {
		t.Errorf("created: %+v", obj)
	} else if content("music/song.mp3") != "abc" {
		t.Errorf("content: %q", content("music/song.mp3"))
	}

	// A nil body creates an empty object, and an object is replaced
	if obj, err := create(schema.CreateObjectRequest{ObjectKey: key("/empty.txt")}); err != nil || *obj.Size != 0 {
		t.Errorf("empty: %+v, %v", obj, err)
	}
	if _, err := create(schema.CreateObjectRequest{ObjectKey: key("/old.txt"), Body: strings.NewReader("new")}); err != nil || content("old.txt") != "new" {
		t.Errorf("replace: %v, %q", err, content("old.txt"))
	}

	// IfNotExists and IfExists
	if _, err := create(schema.CreateObjectRequest{ObjectKey: key("/old.txt"), Body: strings.NewReader("x"), IfNotExists: true}); !errors.Is(err, filer.ErrConflict) || content("old.txt") != "new" {
		t.Errorf("if not exists: %v", err)
	}
	if _, err := create(schema.CreateObjectRequest{ObjectKey: key("/missing.txt"), Body: strings.NewReader("x"), IfExists: true}); !errors.Is(err, filer.ErrPreconditionFailed) || content("missing.txt") != "<missing>" {
		t.Errorf("if exists: %v", err)
	}
	if _, err := create(schema.CreateObjectRequest{ObjectKey: key("/old.txt"), IfExists: true, IfNotExists: true}); !errors.Is(err, filer.ErrBadParameter) {
		t.Errorf("both: %v", err)
	}

	// Objects can't be created at the root, over a directory, at a path which
	// isn't valid, where they'd be skipped, nor in a package, nor with metadata
	for path, want := range map[string]error{
		"/":                             fs.ErrInvalid,
		"/music":                        filer.ErrConflict,
		"/music/../x.txt":               fs.ErrInvalid,
		"/music//x.txt":                 fs.ErrInvalid,
		"/.hidden":                      filer.ErrUnprocessableContent,
		"/music/Thumbs.db":              filer.ErrUnprocessableContent,
		"/Disk.sparsebundle/bands/404e": filer.ErrUnprocessableContent,
	} {
		if _, err := create(schema.CreateObjectRequest{ObjectKey: key(path), Body: strings.NewReader("x")}); !errors.Is(err, want) {
			t.Errorf("%s: %v, want %v", path, err, want)
		}
	}
	if _, err := create(schema.CreateObjectRequest{ObjectKey: key("/meta.txt"), ObjectMeta: schema.ObjectMeta{Meta: []schema.Meta{{}}}}); !errors.Is(err, filer.ErrNotImplemented) {
		t.Errorf("meta: %v", err)
	}

	// A context which is cancelled while the body is read leaves the object
	// unchanged
	cancelCtx, cancel := context.WithCancel(ctx)
	body := io.MultiReader(strings.NewReader("part"), readerFunc(func([]byte) (int, error) {
		cancel()
		return 0, nil
	}), strings.NewReader("rest"))
	if _, err := self.CreateObject(cancelCtx, schema.CreateObjectRequest{ObjectKey: key("/old.txt"), Body: body}); !errors.Is(err, context.Canceled) || content("old.txt") != "new" {
		t.Errorf("cancelled: %v, %q", err, content("old.txt"))
	}

	// A filesystem which isn't writable is read-only, and a closed one is closed
	readonly, err := NewFS("test", fstest.MapFS{}, nil)
	if err != nil {
		t.Fatal(err)
	} else if _, err := readonly.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: key("/x.txt")}); !errors.Is(err, filer.ErrForbidden) {
		t.Errorf("read-only: %v", err)
	}
	self.Close()
	if _, err := create(schema.CreateObjectRequest{ObjectKey: key("/x.txt")}); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("closed: %v", err)
	}
}

// readerFunc is a reader which calls a function
type readerFunc func([]byte) (int, error)

func (fn readerFunc) Read(data []byte) (int, error) {
	return fn(data)
}

func Test_CreateObjectContentType(t *testing.T) {
	files := &memFS{MapFS: fstest.MapFS{"old.txt": {Data: []byte("old")}}}
	self, err := NewFS("test", files, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// A content type is the type of the extension, ignoring parameters and
	// case, or it isn't supported, as it wouldn't be returned
	for path, types := range map[string]map[string]bool{
		"/a.txt":    {"": true, "text/plain": true, "text/plain; charset=utf-8": true, "TEXT/PLAIN": true, "image/png": false, "not a type": false},
		"/README":   {"": true, "text/plain": false},
		"/song.mp3": {"audio/mpeg": true, "audio/mp4": false},
	} {
		for contentType, ok := range types {
			_, err := self.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: path}, ObjectMeta: schema.ObjectMeta{ContentType: contentType}})
			if ok && err != nil {
				t.Errorf("%s %q: %v", path, contentType, err)
			} else if !ok && !errors.Is(err, filer.ErrNotImplemented) {
				t.Errorf("%s %q: %v, want not implemented", path, contentType, err)
			}
		}
	}
}

func Test_CreateObjectCancelledAtEnd(t *testing.T) {
	files := &memFS{MapFS: fstest.MapFS{"old.txt": {Data: []byte("old")}}}
	self, err := NewFS("test", files, nil)
	if err != nil {
		t.Fatal(err)
	}

	// A context which is cancelled during the read which returns the last of
	// the body, with io.EOF, leaves the object unchanged
	ctx, cancel := context.WithCancel(context.Background())
	body := readerFunc(func(data []byte) (int, error) {
		cancel()
		return copy(data, "new"), io.EOF
	})
	if _, err := self.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/old.txt"}, Body: body}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	} else if got := string(files.MapFS["old.txt"].Data); got != "old" {
		t.Errorf("content: %q", got)
	}
}
