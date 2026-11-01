package fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
)

func Test_ReadObject(t *testing.T) {
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	files := fstest.MapFS{
		"song.mp3":                     {Data: []byte("abcdef"), ModTime: modtime},
		"music":                        {Mode: fs.ModeDir},
		"Disk.sparsebundle/bands/404d": {Data: []byte("d")},
	}
	self, err := NewFS("test", files, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := func(path string) schema.GetObjectRequest {
		return schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: path}}
	}
	readAll := func(r io.Reader, buf *bytes.Buffer) error {
		_, err := io.Copy(buf, r)
		return err
	}

	// The object is returned, and its content is read within the function
	read, obj, err := self.ReadObject(ctx, key("/song.mp3"))
	if err != nil {
		t.Fatal(err)
	} else if obj.Path != "/song.mp3" || *obj.Size != 6 || obj.ContentType != "audio/mpeg" {
		t.Errorf("object: %+v", obj)
	}
	var buf bytes.Buffer
	if err := read(ctx, func(r io.Reader) error { return readAll(r, &buf) }); err != nil {
		t.Fatal(err)
	} else if buf.String() != "abcdef" {
		t.Errorf("content: %q", buf.String())
	}

	// The function's error is returned
	errRead := errors.New("read")
	if err := read(ctx, func(io.Reader) error { return errRead }); !errors.Is(err, errRead) {
		t.Errorf("function: %v", err)
	}

	// Directories, and packages, can't be read, and files in packages don't exist
	if _, _, err := self.ReadObject(ctx, key("/music")); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("directory: %v", err)
	}
	if _, _, err := self.ReadObject(ctx, key("/Disk.sparsebundle")); !errors.Is(err, filer.ErrNotImplemented) {
		t.Errorf("package: %v", err)
	}
	if _, _, err := self.ReadObject(ctx, key("/Disk.sparsebundle/bands/404d")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("in package: %v", err)
	}

	// A context which is cancelled while reading stops the read
	cancelCtx, cancel := context.WithCancel(ctx)
	err = read(cancelCtx, func(r io.Reader) error {
		if _, err := r.Read(make([]byte, 1)); err != nil {
			return err
		}
		cancel()
		_, err := io.ReadAll(r)
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
	if err := read(cancelCtx, func(io.Reader) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled before: %v", err)
	}

	// A file which changed since the object was returned isn't read
	files["song.mp3"] = &fstest.MapFile{Data: []byte("changed"), ModTime: modtime}
	if err := read(ctx, func(io.Reader) error { return nil }); !errors.Is(err, filer.ErrPreconditionFailed) {
		t.Errorf("changed: %v", err)
	}
	read, _, err = self.ReadObject(ctx, key("/song.mp3"))
	if err != nil {
		t.Fatal(err)
	}

	// Closing the filesystem while reading stops the read, and the function
	// can't be called after it's closed
	err = read(ctx, func(r io.Reader) error {
		if _, err := r.Read(make([]byte, 1)); err != nil {
			return err
		}
		self.Close()
		_, err := io.ReadAll(r)
		return err
	})
	if !errors.Is(err, fs.ErrClosed) {
		t.Errorf("closed: %v", err)
	}
	if err := read(ctx, func(io.Reader) error { return nil }); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("closed before: %v", err)
	}
	if _, _, err := self.ReadObject(ctx, key("/song.mp3")); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("closed read: %v", err)
	}
	if err := self.Close(); err != nil {
		t.Errorf("close twice: %v", err)
	}
}

func Test_ReadObjectConditional(t *testing.T) {
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	self, err := NewFS("test", fstest.MapFS{"song.mp3": {Data: []byte("abc"), ModTime: modtime}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := schema.ObjectKey{Path: "/song.mp3"}

	// An object which hasn't changed isn't read, as GetObject doesn't return it
	for _, req := range []schema.GetObjectRequest{
		{ObjectKey: key, IfNoneMatch: []string{"*"}},
		{ObjectKey: key, IfModifiedSince: modtime},
	} {
		if read, _, err := self.ReadObject(ctx, req); !errors.Is(err, filer.ErrNotModified) || read != nil {
			t.Errorf("%+v: %v", req, err)
		}
	}

	// An object which has changed is read
	if read, _, err := self.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: key, IfModifiedSince: modtime.Add(-time.Hour)}); err != nil || read == nil {
		t.Errorf("modified: %v", err)
	}
}
