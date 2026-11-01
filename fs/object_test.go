package fs

import (
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
)

func Test_objectFromInfo(t *testing.T) {
	o, err := parseOpts(nil)
	if err != nil {
		t.Fatal(err)
	}
	self := &FS{opt: o}
	modtime := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	fsys := fstest.MapFS{
		"music/song.mp3":               {Data: []byte("abc"), ModTime: modtime},
		"Disk.sparsebundle/Info.plist": {Data: []byte("x")},
		"Disk.sparsebundle":            {Mode: fs.ModeDir, ModTime: modtime},
		"music":                        {Mode: fs.ModeDir, ModTime: modtime},
		"music/unknown.zzz":            {Data: []byte("")},
	}
	stat := func(name string) fs.FileInfo {
		info, err := fs.Stat(fsys, name)
		if err != nil {
			t.Fatal(err)
		}
		return info
	}

	// A file has its size, modification time and type
	obj := self.objectFromInfo("music/song.mp3", stat("music/song.mp3"), objectType("music/song.mp3"))
	if obj.Path != "/music/song.mp3" || obj.IsDir || obj.Size == nil || *obj.Size != 3 || !obj.ModTime.Equal(modtime) || obj.ContentType != "audio/mpeg" {
		t.Errorf("file: %+v", obj)
	}

	// A file of an unknown type has none
	if obj := self.objectFromInfo("music/unknown.zzz", stat("music/unknown.zzz"), objectType("music/unknown.zzz")); obj.ContentType != "" {
		t.Errorf("unknown: %q", obj.ContentType)
	}

	// A directory has neither a size nor a modification time
	obj = self.objectFromInfo("music", stat("music"), objectType("music"))
	if obj.Path != "/music" || !obj.IsDir || obj.Size != nil || !obj.ModTime.IsZero() || obj.ContentType != schema.DirectoryType {
		t.Errorf("directory: %+v", obj)
	}

	// A package is a file of its type, without a size
	obj = self.objectFromInfo("Disk.sparsebundle", stat("Disk.sparsebundle"), objectType("Disk.sparsebundle"))
	if obj.IsDir || obj.Size != nil || !obj.ModTime.Equal(modtime) || obj.ContentType != "application/x-apple-sparsebundle" {
		t.Errorf("package: %+v", obj)
	}

	// The root is "/"
	if obj := self.objectFromInfo(".", stat("."), ""); obj.Path != "/" || !obj.IsDir {
		t.Errorf("root: %+v", obj)
	}
}
