package file

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"testing"
)

func TestWritableFS_Open(t *testing.T) {
	root := t.TempDir()

	// Seed a file directly via os so we can test Open via fs.FS
	f, err := os.Create(root + "/seed.txt")
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("hello")
	f.Close()

	wfs, err := newWritableFS(root)
	if err != nil {
		t.Fatal(err)
	}

	rf, err := wfs.Open("seed.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	data, err := io.ReadAll(rf)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Errorf("got %q, want %q", string(data), "hello")
	}
}

func TestWritableFS_Create(t *testing.T) {
	wfs, err := newWritableFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	f, err := wfs.Create("new.txt")
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("world")
	f.Close()

	data, err := fs.ReadFile(wfs, "new.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "world" {
		t.Errorf("got %q, want %q", string(data), "world")
	}
}

func TestWritableFS_Create_MakesParentDirs(t *testing.T) {
	wfs, err := newWritableFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	f, err := wfs.Create("a/b/c.txt")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	if _, err := fs.Stat(wfs, "a/b/c.txt"); err != nil {
		t.Fatal(err)
	}
}

func TestWritableFS_Create_InvalidPath(t *testing.T) {
	wfs, err := newWritableFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	cases := []string{"/absolute", "../escape", ""}
	for _, name := range cases {
		if _, err := wfs.Create(name); err == nil {
			t.Errorf("Create(%q): expected error, got nil", name)
		}
	}
}

func TestWritableFS_MkdirAll(t *testing.T) {
	wfs, err := newWritableFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := wfs.MkdirAll("x/y/z", 0o755); err != nil {
		t.Fatal(err)
	}

	info, err := fs.Stat(wfs, "x/y/z")
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Error("expected a directory")
	}
}

func TestWritableFS_MkdirAll_InvalidPath(t *testing.T) {
	wfs, err := newWritableFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := wfs.MkdirAll("/absolute", 0o755); err == nil {
		t.Error("expected error for absolute path, got nil")
	}
}

func TestWritableFS_Remove(t *testing.T) {
	wfs, err := newWritableFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	f, err := wfs.Create("todelete.txt")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	if err := wfs.Remove("todelete.txt"); err != nil {
		t.Fatal(err)
	}

	_, err = fs.Stat(wfs, "todelete.txt")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestWritableFS_RemoveAll(t *testing.T) {
	wfs, err := newWritableFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"tree/a.txt", "tree/b/c.txt"} {
		f, err := wfs.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}

	if err := wfs.RemoveAll("tree"); err != nil {
		t.Fatal(err)
	}

	_, err = fs.Stat(wfs, "tree")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("expected ErrNotExist after RemoveAll, got %v", err)
	}
}

func TestWritableFS_Remove_InvalidPath(t *testing.T) {
	wfs, err := newWritableFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	if err := wfs.Remove("/absolute"); err == nil {
		t.Error("expected error for absolute path, got nil")
	}
}
