package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sync"
	"testing"
	"testing/fstest"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

func Test_ListObjects(t *testing.T) {
	self, err := NewFS("test", fstest.MapFS{
		"a.txt":                        {Data: []byte("a")},
		"a-b.txt":                      {Data: []byte("ab")},
		"a/b.txt":                      {Data: []byte("b")},
		"a0.txt":                       {Data: []byte("0")},
		".hidden/c.txt":                {Data: []byte("c")},
		"Thumbs.db":                    {Data: []byte("t")},
		"Disk.sparsebundle/bands/404d": {Data: []byte("d")},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	list := func(req schema.ListObjectsRequest) []string {
		iter, err := self.ListObjects(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()
		paths := []string{}
		for {
			obj, err := iter.Next(context.Background())
			if errors.Is(err, io.EOF) {
				return paths
			} else if err != nil {
				t.Fatal(err)
			}
			paths = append(paths, obj.Path)
		}
	}
	equal := func(name string, got, want []string) {
		t.Helper()
		if len(got) != len(want) {
			t.Errorf("%s: got %q, want %q", name, got, want)
			return
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("%s: got %q, want %q", name, got, want)
				return
			}
		}
	}

	// Files and directories are listed, in S3's order, without skipped
	// names, and a package is a file, which isn't descended into
	equal("list", list(schema.ListObjectsRequest{}), []string{"/Disk.sparsebundle", "/a-b.txt", "/a.txt", "/a", "/a0.txt"})
	equal("recursive", list(schema.ListObjectsRequest{Recursive: true}), []string{"/Disk.sparsebundle", "/a-b.txt", "/a.txt", "/a", "/a/b.txt", "/a0.txt"})

	// Listing starts after the start path, within a directory too
	equal("start", list(schema.ListObjectsRequest{Recursive: true, StartAfter: "/a/"}), []string{"/a/b.txt", "/a0.txt"})
	equal("start within", list(schema.ListObjectsRequest{Recursive: true, StartAfter: "/a.txt"}), []string{"/a", "/a/b.txt", "/a0.txt"})

	// Listing a file, or a package, returns just it
	equal("file", list(schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a/b.txt"}}), []string{"/a/b.txt"})
	equal("package", list(schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/Disk.sparsebundle"}, Recursive: true}), []string{"/Disk.sparsebundle"})

	// Skipped files, and files in packages, don't exist
	for _, path := range []string{"/.hidden/c.txt", "/Thumbs.db", "/Disk.sparsebundle/bands/404d"} {
		if _, err := self.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: path}}); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s: %v", path, err)
		}
	}
}

func Test_ListObjectsConcurrent(t *testing.T) {
	files := fstest.MapFS{}
	for i := range 100 {
		files[fmt.Sprintf("dir%d/file%d.txt", i%10, i)] = &fstest.MapFile{Data: []byte("x")}
	}
	self, err := NewFS("test", files, nil)
	if err != nil {
		t.Fatal(err)
	}
	iter, err := self.ListObjects(context.Background(), schema.ListObjectsRequest{Recursive: true})
	if err != nil {
		t.Fatal(err)
	}

	// Each object is returned once, to one of the goroutines calling Next
	var mu sync.Mutex
	var wg sync.WaitGroup
	seen := map[string]int{}
	for range 8 {
		wg.Go(func() {
			for {
				obj, err := iter.Next(context.Background())
				if err != nil {
					if !errors.Is(err, io.EOF) && !errors.Is(err, fs.ErrClosed) {
						t.Error(err)
					}
					return
				}
				mu.Lock()
				seen[obj.Path]++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(seen) != 110 {
		t.Errorf("got %d objects, want 110", len(seen))
	}
	for path, n := range seen {
		if n != 1 {
			t.Errorf("%s returned %d times", path, n)
		}
	}

	// Closing while Next is called stops it
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	} else if _, err := iter.Next(context.Background()); !errors.Is(err, fs.ErrClosed) {
		t.Errorf("after close: %v", err)
	}
}
