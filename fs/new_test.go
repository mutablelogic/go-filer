package fs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
)

func Test_New(t *testing.T) {
	dir := t.TempDir()
	self, err := New("test", dir, nil)
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
	if _, err := New("test", filepath.Join(dir, "music", "song.mp3"), nil); err == nil {
		t.Error("file as root")
	} else if _, err := New("test", filepath.Join(dir, "missing"), nil); err == nil {
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
