package fs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"testing/iotest"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// symlink creates a symbolic link, skipping the test on Windows when the
// process does not have permission to create symbolic links
func symlink(t *testing.T, oldname, newname string) {
	t.Helper()
	if err := os.Symlink(oldname, newname); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("cannot create symbolic links: %v", err)
		}
		require.NoError(t, err)
	}
}

// invalidPaths returns the paths which are invalid on this platform. Paths
// with a backslash or colon are only invalid on Windows, as elsewhere they're
// ordinary characters in file names.
func invalidPaths(paths ...string) []string {
	if runtime.GOOS == "windows" {
		return paths
	}
	return slices.DeleteFunc(paths, func(path string) bool {
		return strings.ContainsAny(path, `\:`)
	})
}

func Test_WriterFS_Open(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello, world"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "subdir", "nested.txt"), []byte("nested"), 0o644))

	// On Windows, a directory listing reports file information cached in the
	// parent directory, which NTFS updates lazily. Writing nested.txt changes
	// the modification time of subdir, but not the cached copy, and TestFS
	// fails as the listing and Stat disagree. Setting the times explicitly
	// updates the cached copies.
	modtime := time.Now().Add(-time.Hour).Truncate(time.Second)
	for _, name := range []string{"hello.txt", "subdir/nested.txt", "subdir"} {
		require.NoError(t, os.Chtimes(filepath.Join(dir, filepath.FromSlash(name)), modtime, modtime))
	}

	wfs, err := newWriterFS(dir)
	require.NoError(t, err)
	t.Cleanup(func() { wfs.Close() })

	t.Run("Conformance", func(t *testing.T) {
		require.NoError(t, fstest.TestFS(wfs, "hello.txt", "subdir/nested.txt"))
	})

	t.Run("ReadFile", func(t *testing.T) {
		f, err := wfs.Open("subdir/nested.txt")
		require.NoError(t, err)
		defer f.Close()
		data, err := io.ReadAll(f)
		require.NoError(t, err)
		assert.Equal(t, "nested", string(data))
	})

	t.Run("NotFound", func(t *testing.T) {
		_, err := wfs.Open("missing.txt")
		assert.ErrorIs(t, err, fs.ErrNotExist)
		var pathErr *fs.PathError
		require.ErrorAs(t, err, &pathErr)
		assert.Equal(t, "missing.txt", pathErr.Path)
	})

	t.Run("InvalidPath", func(t *testing.T) {
		for _, path := range invalidPaths("/hello.txt", "../hello.txt", "subdir/../hello.txt", "./hello.txt", "", `subdir\nested.txt`, "c:hello.txt", "hello.txt:stream") {
			_, err := wfs.Open(path)
			assert.ErrorIs(t, err, fs.ErrInvalid, "path %q", path)
		}
	})

	t.Run("SymlinkEscape", func(t *testing.T) {
		outside := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o644))
		symlink(t, filepath.Join(outside, "secret.txt"), filepath.Join(dir, "escape.txt"))
		t.Cleanup(func() { os.Remove(filepath.Join(dir, "escape.txt")) })

		_, err := wfs.Open("escape.txt")
		assert.Error(t, err)
	})
}

func Test_WriterFS_Close(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello, world"), 0o644))

	wfs, err := newWriterFS(dir)
	require.NoError(t, err)

	// Open succeeds before close
	f, err := wfs.Open("hello.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Close is idempotent
	require.NoError(t, wfs.Close())
	require.NoError(t, wfs.Close())

	// Open fails after close
	_, err = wfs.Open("hello.txt")
	assert.ErrorIs(t, err, fs.ErrClosed)
}

func Test_WriterFS_Create(t *testing.T) {
	dir := t.TempDir()
	wfs, err := newWriterFS(dir)
	require.NoError(t, err)
	t.Cleanup(func() { wfs.Close() })

	write := func(t *testing.T, path, data string) fs.FileInfo {
		t.Helper()
		info, err := wfs.Create(path, strings.NewReader(data), filer.CreateOrReplace, time.Time{})
		require.NoError(t, err)
		return info
	}
	readFile := func(t *testing.T, path string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(path)))
		require.NoError(t, err)
		return string(data)
	}

	// No temporary files are left behind by any test
	t.Cleanup(func() {
		filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err == nil && strings.HasPrefix(d.Name(), tempPrefix) {
				t.Errorf("temporary file left behind: %q", path)
			}
			return nil
		})
	})

	t.Run("File", func(t *testing.T) {
		info := write(t, "hello.txt", "hello, world")
		assert.Equal(t, "hello, world", readFile(t, "hello.txt"))
		assert.False(t, info.IsDir())
		assert.Equal(t, int64(len("hello, world")), info.Size())
	})

	t.Run("NilBody", func(t *testing.T) {
		info, err := wfs.Create("empty.txt", nil, filer.CreateOrReplace, time.Time{})
		require.NoError(t, err)
		assert.Equal(t, int64(0), info.Size())
		assert.Equal(t, "", readFile(t, "empty.txt"))
	})

	t.Run("Name", func(t *testing.T) {
		// The information reports the target name, not the temporary file
		info := write(t, "sub/named.txt", "data")
		assert.Equal(t, "named.txt", info.Name())
	})

	t.Run("ModTime", func(t *testing.T) {
		// Whole seconds, which every filesystem can store exactly
		modtime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
		for _, mode := range []filer.CreateMode{filer.CreateOrReplace, filer.CreateOnly} {
			path := fmt.Sprintf("modtime-%v.txt", mode)
			info, err := wfs.Create(path, strings.NewReader("data"), mode, modtime)
			require.NoError(t, err)
			assert.True(t, info.ModTime().Equal(modtime), "returned %v", info.ModTime())

			stat, err := os.Stat(filepath.Join(dir, path))
			require.NoError(t, err)
			assert.True(t, stat.ModTime().Equal(modtime), "stored %v", stat.ModTime())
		}
	})

	t.Run("ZeroModTime", func(t *testing.T) {
		// A zero time leaves the time the file was written
		before := time.Now().Add(-time.Minute)
		info := write(t, "now.txt", "data")
		assert.True(t, info.ModTime().After(before), "returned %v", info.ModTime())
	})

	t.Run("IntermediateDirectories", func(t *testing.T) {
		write(t, "a/b/c/nested.txt", "nested")
		assert.Equal(t, "nested", readFile(t, "a/b/c/nested.txt"))
	})

	t.Run("Replace", func(t *testing.T) {
		write(t, "replace.txt", "a longer piece of text")
		write(t, "replace.txt", "short")
		assert.Equal(t, "short", readFile(t, "replace.txt"))
	})

	t.Run("FailedReplaceKeepsOriginal", func(t *testing.T) {
		write(t, "original.txt", "original")
		bodyErr := errors.New("body failed")
		_, err := wfs.Create("original.txt", io.MultiReader(strings.NewReader("partial"), iotest.ErrReader(bodyErr)), filer.CreateOrReplace, time.Time{})
		assert.ErrorIs(t, err, bodyErr)
		assert.Equal(t, "original", readFile(t, "original.txt"))
	})

	t.Run("FailedCreateRemovesDirectories", func(t *testing.T) {
		bodyErr := errors.New("body failed")
		_, err := wfs.Create("failed/dir/file.txt", iotest.ErrReader(bodyErr), filer.CreateOrReplace, time.Time{})
		assert.ErrorIs(t, err, bodyErr)
		_, err = os.Stat(filepath.Join(dir, "failed"))
		assert.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("Exclusive", func(t *testing.T) {
		_, err := wfs.Create("exclusive.txt", strings.NewReader("first"), filer.CreateOnly, time.Time{})
		require.NoError(t, err)
		_, err = wfs.Create("exclusive.txt", strings.NewReader("second"), filer.CreateOnly, time.Time{})
		assert.ErrorIs(t, err, fs.ErrExist)
		assert.Equal(t, "first", readFile(t, "exclusive.txt"))
	})

	t.Run("ReplaceOnly", func(t *testing.T) {
		// A file which doesn't exist isn't created, and leaves nothing
		// behind, and one which does is replaced
		_, err := wfs.Create("replace-only/missing.txt", strings.NewReader("data"), filer.ReplaceOnly, time.Time{})
		assert.ErrorIs(t, err, fs.ErrNotExist)
		_, err = os.Stat(filepath.Join(dir, "replace-only"))
		assert.ErrorIs(t, err, fs.ErrNotExist)

		_, err = wfs.Create("replace-only.txt", strings.NewReader("first"), filer.CreateOnly, time.Time{})
		require.NoError(t, err)
		_, err = wfs.Create("replace-only.txt", strings.NewReader("second"), filer.ReplaceOnly, time.Time{})
		require.NoError(t, err)
		assert.Equal(t, "second", readFile(t, "replace-only.txt"))
	})

	t.Run("ExclusiveConcurrent", func(t *testing.T) {
		// Of many concurrent exclusive creates of the same path, exactly one
		// succeeds, and the file contains its body
		const n = 20
		var wg sync.WaitGroup
		var mu sync.Mutex
		winners := []string{}
		for i := range n {
			wg.Go(func() {
				body := fmt.Sprint("writer ", i)
				_, err := wfs.Create("race/file.txt", strings.NewReader(body), filer.CreateOnly, time.Time{})
				if err == nil {
					mu.Lock()
					winners = append(winners, body)
					mu.Unlock()
				} else {
					assert.ErrorIs(t, err, fs.ErrExist)
				}
			})
		}
		wg.Wait()
		require.Len(t, winners, 1)
		assert.Equal(t, winners[0], readFile(t, "race/file.txt"))
	})

	t.Run("ReadBack", func(t *testing.T) {
		write(t, "x/y.txt", "roundtrip")
		data, err := fs.ReadFile(wfs, "x/y.txt")
		require.NoError(t, err)
		assert.Equal(t, "roundtrip", string(data))
	})

	t.Run("InvalidPath", func(t *testing.T) {
		for _, path := range invalidPaths(".", "", "/abs.txt", "../escape.txt", "a/../b.txt", "./c.txt", ".hidden", ".hidden/file.txt", "a/.hidden", "a/.b/c.txt", "...", `a\b.txt`, `a\.hidden`, `a\..\b.txt`, "c:file.txt", "file.txt:stream") {
			_, err := wfs.Create(path, strings.NewReader("data"), filer.CreateOrReplace, time.Time{})
			assert.ErrorIs(t, err, fs.ErrInvalid, "path %q", path)
		}
	})

	t.Run("ExistingDirectory", func(t *testing.T) {
		require.NoError(t, os.Mkdir(filepath.Join(dir, "isdir"), 0o755))
		for _, mode := range []filer.CreateMode{filer.CreateOrReplace, filer.CreateOnly, filer.ReplaceOnly} {
			_, err := wfs.Create("isdir", strings.NewReader("data"), mode, time.Time{})
			assert.Error(t, err)
			info, err := os.Stat(filepath.Join(dir, "isdir"))
			require.NoError(t, err)
			assert.True(t, info.IsDir())
		}
	})

	t.Run("ParentIsFile", func(t *testing.T) {
		write(t, "file.txt", "data")
		_, err := wfs.Create("file.txt/child.txt", strings.NewReader("data"), filer.CreateOrReplace, time.Time{})
		assert.Error(t, err)
	})

	t.Run("SymlinkEscape", func(t *testing.T) {
		outside := t.TempDir()
		symlink(t, outside, filepath.Join(dir, "link"))

		// Writing through a symlinked directory which points outside the root
		_, err := wfs.Create("link/escape.txt", strings.NewReader("data"), filer.CreateOrReplace, time.Time{})
		assert.Error(t, err)
		_, err = os.Stat(filepath.Join(outside, "escape.txt"))
		assert.ErrorIs(t, err, fs.ErrNotExist)

		// Creating directories beneath the symlink
		_, err = wfs.Create("link/sub/escape.txt", strings.NewReader("data"), filer.CreateOrReplace, time.Time{})
		assert.Error(t, err)
		_, err = os.Stat(filepath.Join(outside, "sub"))
		assert.ErrorIs(t, err, fs.ErrNotExist)

		// Nothing, including temporary files, is written outside the root
		entries, err := os.ReadDir(outside)
		require.NoError(t, err)
		assert.Empty(t, entries)

		// The symlink itself is not removed when tidying up
		info, err := os.Lstat(filepath.Join(dir, "link"))
		require.NoError(t, err)
		assert.Equal(t, fs.ModeSymlink, info.Mode().Type())
	})

	t.Run("CleanupOnFailure", func(t *testing.T) {
		// A filename which exceeds the maximum name length fails after the
		// intermediate directories have been created
		longname := strings.Repeat("x", 300)

		_, err := wfs.Create("p/q/r/"+longname, strings.NewReader("data"), filer.CreateOrReplace, time.Time{})
		require.Error(t, err)
		_, err = os.Stat(filepath.Join(dir, "p"))
		assert.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("CleanupKeepsNonEmpty", func(t *testing.T) {
		longname := strings.Repeat("x", 300)
		write(t, "keep/file.txt", "data")

		_, err := wfs.Create("keep/new/"+longname, strings.NewReader("data"), filer.CreateOrReplace, time.Time{})
		require.Error(t, err)
		_, err = os.Stat(filepath.Join(dir, "keep", "new"))
		assert.ErrorIs(t, err, fs.ErrNotExist)
		_, err = os.Stat(filepath.Join(dir, "keep", "file.txt"))
		assert.NoError(t, err)
	})

	t.Run("Closed", func(t *testing.T) {
		wfs, err := newWriterFS(t.TempDir())
		require.NoError(t, err)
		require.NoError(t, wfs.Close())
		_, err = wfs.Create("hello.txt", strings.NewReader("data"), filer.CreateOrReplace, time.Time{})
		assert.ErrorIs(t, err, fs.ErrClosed)
	})
}

func Test_WriterFS_Delete(t *testing.T) {
	dir := t.TempDir()
	wfs, err := newWriterFS(dir)
	require.NoError(t, err)
	t.Cleanup(func() { wfs.Close() })

	write := func(t *testing.T, path string) {
		t.Helper()
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, path)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, path), []byte("data"), 0o644))
	}
	exists := func(path string) bool {
		_, err := os.Lstat(filepath.Join(dir, path))
		return err == nil
	}

	t.Run("File", func(t *testing.T) {
		write(t, "hello.txt")
		require.NoError(t, wfs.Delete("hello.txt"))
		assert.False(t, exists("hello.txt"))
	})

	t.Run("RemovesEmptyParents", func(t *testing.T) {
		write(t, "a/b/c/nested.txt")
		require.NoError(t, wfs.Delete("a/b/c/nested.txt"))
		assert.False(t, exists("a"))
	})

	t.Run("KeepsNonEmptyParents", func(t *testing.T) {
		write(t, "keep/sibling.txt")
		write(t, "keep/sub/nested.txt")
		require.NoError(t, wfs.Delete("keep/sub/nested.txt"))
		assert.False(t, exists("keep/sub"))
		assert.True(t, exists("keep/sibling.txt"))
	})

	t.Run("NotFound", func(t *testing.T) {
		assert.ErrorIs(t, wfs.Delete("missing.txt"), fs.ErrNotExist)
	})

	t.Run("Directory", func(t *testing.T) {
		write(t, "tree/a.txt")
		write(t, "tree/sub/b.txt")
		write(t, "tree/sub/deeper/c.txt")
		write(t, "tree/.hidden/d.txt")
		require.NoError(t, wfs.Delete("tree"))
		assert.False(t, exists("tree"))
	})

	t.Run("NestedDirectory", func(t *testing.T) {
		write(t, "outer/keep.txt")
		write(t, "outer/inner/a.txt")
		require.NoError(t, wfs.Delete("outer/inner"))
		assert.False(t, exists("outer/inner"))
		assert.True(t, exists("outer/keep.txt"))
	})

	t.Run("SymlinkToDirectory", func(t *testing.T) {
		outside := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(outside, "keep.txt"), []byte("data"), 0o644))
		symlink(t, outside, filepath.Join(dir, "linkdir"))

		// The link is removed, but not the directory it points to
		require.NoError(t, wfs.Delete("linkdir"))
		assert.False(t, exists("linkdir"))
		_, err := os.Stat(filepath.Join(outside, "keep.txt"))
		assert.NoError(t, err)
	})

	t.Run("SymlinkInsideDirectory", func(t *testing.T) {
		outside := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(outside, "keep.txt"), []byte("data"), 0o644))
		write(t, "withlink/a.txt")
		symlink(t, outside, filepath.Join(dir, "withlink", "link"))

		// Links inside the directory are removed, not followed
		require.NoError(t, wfs.Delete("withlink"))
		assert.False(t, exists("withlink"))
		_, err := os.Stat(filepath.Join(outside, "keep.txt"))
		assert.NoError(t, err)
	})

	t.Run("RemovesEmptyParentsOfDirectory", func(t *testing.T) {
		write(t, "x/y/z/a.txt")
		require.NoError(t, wfs.Delete("x/y/z"))
		assert.False(t, exists("x"))
	})

	t.Run("InvalidPath", func(t *testing.T) {
		for _, path := range invalidPaths(".", "", "/abs.txt", "../escape.txt", "a/../b.txt", "./c.txt", ".hidden", ".hidden/file.txt", "a/.hidden", "a/.b/c.txt", "...", `a\b.txt`, `a\.hidden`, `a\..\b.txt`, "c:file.txt", "file.txt:stream") {
			assert.ErrorIs(t, wfs.Delete(path), fs.ErrInvalid, "path %q", path)
		}
	})

	t.Run("HiddenFile", func(t *testing.T) {
		write(t, ".hidden/file.txt")
		assert.ErrorIs(t, wfs.Delete(".hidden/file.txt"), fs.ErrInvalid)
		assert.True(t, exists(".hidden/file.txt"))
	})

	t.Run("Symlink", func(t *testing.T) {
		outside := t.TempDir()
		target := filepath.Join(outside, "target.txt")
		require.NoError(t, os.WriteFile(target, []byte("data"), 0o644))
		symlink(t, target, filepath.Join(dir, "link.txt"))

		// The link is removed, but not the file it points to
		require.NoError(t, wfs.Delete("link.txt"))
		assert.False(t, exists("link.txt"))
		_, err := os.Stat(target)
		assert.NoError(t, err)
	})

	t.Run("SymlinkEscape", func(t *testing.T) {
		outside := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("data"), 0o644))
		symlink(t, outside, filepath.Join(dir, "linkdir"))

		// Cannot delete through a symlinked directory pointing outside the root
		assert.Error(t, wfs.Delete("linkdir/secret.txt"))
		_, err := os.Stat(filepath.Join(outside, "secret.txt"))
		assert.NoError(t, err)
	})

	t.Run("Closed", func(t *testing.T) {
		wfs, err := newWriterFS(t.TempDir())
		require.NoError(t, err)
		require.NoError(t, wfs.Close())
		assert.ErrorIs(t, wfs.Delete("hello.txt"), fs.ErrClosed)
	})
}

func Test_WriterFS_Stat(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "subdir", "nested.txt"), []byte("nested"), 0o644))

	wfs, err := newWriterFS(dir)
	require.NoError(t, err)
	t.Cleanup(func() { wfs.Close() })

	t.Run("File", func(t *testing.T) {
		info, err := wfs.Stat("subdir/nested.txt")
		require.NoError(t, err)
		assert.Equal(t, "nested.txt", info.Name())
		assert.False(t, info.IsDir())
		assert.Equal(t, int64(len("nested")), info.Size())
	})

	t.Run("Directory", func(t *testing.T) {
		info, err := wfs.Stat("subdir")
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	})

	t.Run("Root", func(t *testing.T) {
		info, err := wfs.Stat(".")
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	})

	t.Run("NotFound", func(t *testing.T) {
		_, err := wfs.Stat("missing.txt")
		assert.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("InvalidPath", func(t *testing.T) {
		for _, path := range invalidPaths("", "/subdir", "../subdir", "subdir/../subdir", `subdir\nested.txt`, "c:subdir") {
			_, err := wfs.Stat(path)
			assert.ErrorIs(t, err, fs.ErrInvalid, "path %q", path)
		}
	})

	t.Run("SymlinkEscape", func(t *testing.T) {
		outside := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o644))
		symlink(t, filepath.Join(outside, "secret.txt"), filepath.Join(dir, "escape.txt"))
		t.Cleanup(func() { os.Remove(filepath.Join(dir, "escape.txt")) })

		_, err := wfs.Stat("escape.txt")
		assert.Error(t, err)
	})

	t.Run("Closed", func(t *testing.T) {
		wfs, err := newWriterFS(t.TempDir())
		require.NoError(t, err)
		require.NoError(t, wfs.Close())
		_, err = wfs.Stat(".")
		assert.ErrorIs(t, err, fs.ErrClosed)
	})
}
