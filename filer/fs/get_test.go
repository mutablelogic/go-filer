package fs

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

func Test_GetObject(t *testing.T) {
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	self, err := NewFS("test", fstest.MapFS{
		"music/song.mp3":               {Data: []byte("abc"), ModTime: modtime},
		".hidden/c.txt":                {Data: []byte("c")},
		"Disk.sparsebundle/bands/404d": {Data: []byte("d")},
	}, nil, filer.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	get := func(path string) (*schema.Object, error) {
		return self.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: path}})
	}

	// A file, with or without a leading "/"
	for _, path := range []string{"/music/song.mp3", "music/song.mp3"} {
		obj, err := get(path)
		if err != nil {
			t.Fatal(err)
		} else if obj.Path != "/music/song.mp3" || obj.Size == nil || *obj.Size != 3 || !obj.ModTime.Equal(modtime) || obj.ContentType != "audio/mpeg" {
			t.Errorf("%s: %+v", path, obj)
		}
	}

	// A directory, the root, and a package
	if obj, err := get("/music"); err != nil || !obj.IsDir || obj.Path != "/music" {
		t.Errorf("directory: %+v, %v", obj, err)
	}
	for _, path := range []string{"", "/"} {
		if obj, err := get(path); err != nil || !obj.IsDir || obj.Path != "/" {
			t.Errorf("root %q: %+v, %v", path, obj, err)
		}
	}
	if obj, err := get("/Disk.sparsebundle"); err != nil || obj.IsDir || obj.ContentType != "application/x-apple-sparsebundle" {
		t.Errorf("package: %+v, %v", obj, err)
	}

	// A path which ends with "/" is a directory, which a file, or a package,
	// isn't
	if obj, err := get("/music/"); err != nil || !obj.IsDir {
		t.Errorf("directory with /: %+v, %v", obj, err)
	}
	for _, path := range []string{"/music/song.mp3/", "/Disk.sparsebundle/"} {
		if _, err := get(path); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s: %v", path, err)
		}
	}

	// Missing, skipped, and in a package, don't exist
	for _, path := range []string{"/missing", "/.hidden/c.txt", "/.hidden", "/Disk.sparsebundle/bands/404d"} {
		if _, err := get(path); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s: %v", path, err)
		}
	}

	// Paths which aren't valid are invalid, whatever the filesystem says
	for _, path := range []string{"/../etc/passwd", "/music//song.mp3", "/music/./song.mp3", "/music//", "//"} {
		if _, err := get(path); !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("%s: %v", path, err)
		}
	}

	// A cancelled context, and a closed filesystem
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := self.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/music"}}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
	self.Close()
	if _, err := get("/music"); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("closed: %v", err)
	}
}

func Test_GetObjectConditional(t *testing.T) {
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 500_000_000, time.UTC)
	self, err := NewFS("test", fstest.MapFS{
		"song.mp3": {Data: []byte("abc"), ModTime: modtime},
		"music":    {Mode: fs.ModeDir},
	}, nil, filer.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	get := func(req schema.GetObjectRequest) error {
		_, err := self.GetObject(context.Background(), req)
		return err
	}
	song := schema.ObjectKey{Path: "/song.mp3"}

	// Modified since a time before it, to the second
	for since, want := range map[time.Time]bool{
		modtime.Add(-time.Second):      false,
		modtime.Truncate(time.Second):  true,
		modtime.Add(-time.Millisecond): true,
		modtime.Add(time.Hour):         true,
	} {
		if err := get(schema.GetObjectRequest{ObjectKey: song, IfModifiedSince: since}); errors.Is(err, filer.ErrNotModified) != want {
			t.Errorf("since %v: %v", since, err)
		}
	}

	// A directory has no modification time, so it's always returned
	if err := get(schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/music"}, IfModifiedSince: modtime}); err != nil {
		t.Errorf("directory: %v", err)
	}

	// "*" matches any object, and an ETag can't match an object without one,
	// and IfNoneMatch is used in place of IfModifiedSince
	if err := get(schema.GetObjectRequest{ObjectKey: song, IfNoneMatch: []string{"*"}}); !errors.Is(err, filer.ErrNotModified) {
		t.Errorf("*: %v", err)
	}
	if err := get(schema.GetObjectRequest{ObjectKey: song, IfNoneMatch: []string{`"abc"`}, IfModifiedSince: modtime.Add(time.Hour)}); err != nil {
		t.Errorf("etag: %v", err)
	}

	// An object with an ETag isn't modified when it matches
	obj := &schema.Object{ObjectAttr: schema.ObjectAttr{ETag: types.Ptr(`"abc"`)}}
	if !obj.NotModified(schema.GetObjectRequest{IfNoneMatch: []string{`"xyz"`, `"abc"`}}) {
		t.Error("matching etag")
	} else if obj.NotModified(schema.GetObjectRequest{IfNoneMatch: []string{`"xyz"`}}) {
		t.Error("other etag")
	}
}
