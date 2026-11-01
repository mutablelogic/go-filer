package fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

func Test_New(t *testing.T) {
	dir := t.TempDir()
	self, err := New("test", dir, nil, filer.Opts{})
	if err != nil {
		t.Fatal(err)
	} else if self.URL().Query().Get("readonly") != "" {
		t.Errorf("url: %v", self.URL())
	}
	ctx := context.Background()

	// An object is created in the directory, and can be got, read and listed
	if _, err := self.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/music/song.mp3"}, Body: strings.NewReader("abc")}); err != nil {
		t.Fatal(err)
	} else if data, err := os.ReadFile(filepath.Join(dir, "music", "song.mp3")); err != nil || string(data) != "abc" {
		t.Errorf("file: %q, %v", data, err)
	}
	if obj, err := self.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/music/song.mp3"}}); err != nil || *obj.Size != 3 {
		t.Errorf("get: %+v, %v", obj, err)
	}
	read, _, err := self.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/music/song.mp3"}})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := read(ctx, func(r io.Reader) error { _, err := io.Copy(&buf, r); return err }); err != nil || buf.String() != "abc" {
		t.Errorf("read: %q, %v", buf.String(), err)
	}
	iter, err := self.ListObjects(ctx, schema.ListObjectsRequest{Recursive: true})
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{}
	for {
		obj, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, obj.Path)
	}
	iter.Close()
	if strings.Join(paths, ",") != "/music,/music/song.mp3" {
		t.Errorf("list: %v", paths)
	}

	// A path which isn't a directory can't be the root
	if _, err := New("test", filepath.Join(dir, "music", "song.mp3"), nil, filer.Opts{}); err == nil {
		t.Error("file as root")
	} else if _, err := New("test", filepath.Join(dir, "missing"), nil, filer.Opts{}); err == nil {
		t.Error("missing root")
	}

	// Closing while objects are created stops them, without a race, and
	// closes the directory
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			for j := 0; ; j++ {
				_, err := self.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/" + strings.Repeat("x", i+1) + ".txt"}, Body: strings.NewReader("data")})
				if errors.Is(err, fs.ErrClosed) {
					return
				} else if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	if err := self.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err := self.Close(); err != nil {
		t.Errorf("close twice: %v", err)
	}
	if _, err := self.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/music"}}); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("closed: %v", err)
	}
}

func Test_URL(t *testing.T) {
	dir := t.TempDir()

	// A filesystem rooted at a directory has its path, and the options which
	// aren't the defaults, even once it's closed
	self, err := New("media", dir, url.Values{"readonly": {"true"}, "skip": {`\.tmp$`}}, filer.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	u := self.URL()
	if u.Scheme != "file" || u.Host != "media" || u.Path != urlPath(self.path) || !strings.HasSuffix(u.Path, filepath.ToSlash(filepath.Base(dir))) {
		t.Errorf("url: %v", u)
	} else if u.Query().Get("readonly") != "true" || u.Query().Get("skip") != `\.tmp$` || u.Query().Has("package") {
		t.Errorf("query: %v", u.RawQuery)
	}
	self.Close()
	if got := self.URL().String(); got != u.String() {
		t.Errorf("closed: %v, want %v", got, u)
	}

	// A filesystem which isn't rooted at a directory has no path
	self, err = NewFS("media", fstest.MapFS{}, nil, filer.Opts{})
	if err != nil {
		t.Fatal(err)
	} else if got := self.URL().String(); got != "fs://media?readonly=true" {
		t.Errorf("url: %v", got)
	}

	// A path is converted to a URL path, with forward slashes, and a leading
	// "/" for a Windows path
	for path, want := range map[string]string{"/mnt/media": "/mnt/media", `C:/media`: "/C:/media"} {
		if got := urlPath(path); got != want {
			t.Errorf("urlPath(%q) = %q, want %q", path, got, want)
		}
	}
}

func Test_Clone(t *testing.T) {
	dir := t.TempDir()
	self, err := New("test", dir, url.Values{"skip": {"^x"}}, filer.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	defer self.Close()

	// The clone is of the directory, with the options changed, and the
	// filesystem isn't changed
	clone, err := self.Clone(context.Background(), url.Values{"readonly": {"true"}})
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Close()
	if got, want := clone.URL().Query(), (url.Values{"skip": {"^x"}, "readonly": {"true"}}); got.Encode() != want.Encode() {
		t.Errorf("clone: got %q, want %q", got, want)
	} else if clone.URL().Path != self.URL().Path {
		t.Errorf("clone: got %q, want %q", clone.URL().Path, self.URL().Path)
	}
	if !clone.(*FS).Readonly() || self.Readonly() {
		t.Error("readonly isn't the clone's")
	}

	// A filesystem which New didn't create can't be cloned, even when it
	// has a path, as opening its path again would lose what it restricts
	for _, fsys := range []fs.FS{fstest.MapFS{}, pathMapFS{fstest.MapFS{}, dir}} {
		other, err := NewFS("test", fsys, nil, filer.Opts{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := other.Clone(context.Background(), nil); !errors.Is(err, filer.ErrNotImplemented) {
			t.Errorf("clone of %T: %v", fsys, err)
		}
	}
}

// pathMapFS is a filesystem of files in memory, with a path
type pathMapFS struct {
	fstest.MapFS
	path string
}

func (fsys pathMapFS) Path() string {
	return fsys.path
}
