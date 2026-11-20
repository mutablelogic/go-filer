package registry

import (
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fsbackend "github.com/mutablelogic/go-filer/backend/fs"
	store "github.com/mutablelogic/go-filer/store"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// cacheFiles returns the names of the caches' databases in the directory
func cacheFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*"+cacheExt))
	require.NoError(t, err)
	result := []string{}
	for _, match := range matches {
		result = append(result, filepath.Base(match))
	}
	slices.Sort(result)
	return result
}

// putCache stores the content as the entry of the key
func putCache(t *testing.T, cache store.CacheStore, key, content string) {
	t.Helper()
	entry, err := cache.Put(t.Context(), key, 0, store.CacheMeta{}, strings.NewReader(content))
	require.NoError(t, err)
	require.NoError(t, entry.Close())
}

// getCache returns the content of the entry of the key, or an error
func getCache(t *testing.T, cache store.CacheStore, key string) (string, error) {
	t.Helper()
	entry, err := cache.Get(t.Context(), key)
	if err != nil {
		return "", err
	}
	defer entry.Close()
	data, err := io.ReadAll(entry)
	return string(data), err
}

func Test_Caches(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	c, err := newCaches(dir)
	require.NoError(t, err)

	// A volume's cache, of its stored URL, is shared by its backends, and
	// kept, once they're closed
	require.NoError(t, c.stored("media", "file://media/a"))
	a, releaseA, _, err := c.acquire(ctx, "media", "file://media/a")
	require.NoError(t, err)
	b, releaseB, _, err := c.acquire(ctx, "media", "file://media/a")
	require.NoError(t, err)
	assert.Same(t, a, b)
	putCache(t, a, "k", "v")
	require.NoError(t, releaseA())
	require.NoError(t, releaseA(), "which is released once")
	v, err := getCache(t, b, "k")
	require.NoError(t, err, "it's open until both are released")
	assert.Equal(t, "v", v)
	require.NoError(t, releaseB())
	files := cacheFiles(t, dir)
	require.Len(t, files, 1)
	assert.True(t, strings.HasPrefix(files[0], "media."))

	// And opened again, of the same URL, with what it had
	a, releaseA, _, err = c.acquire(ctx, "media", "file://media/a")
	require.NoError(t, err)
	v, err = getCache(t, a, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", v)

	// The volume's URL changes, so its cache, which is open, is deleted once
	// it's released, and the new URL's is another
	require.NoError(t, c.stored("media", "file://media/b"))
	b, releaseB, _, err = c.acquire(ctx, "media", "file://media/b")
	require.NoError(t, err)
	_, err = getCache(t, b, "k")
	assert.ErrorIs(t, err, gofiler.ErrNotFound, "it starts again")
	assert.Len(t, cacheFiles(t, dir), 2)
	require.NoError(t, releaseA())
	assert.Len(t, cacheFiles(t, dir), 1, "the old one is deleted, once it's released")
	require.NoError(t, releaseB())
	assert.Len(t, cacheFiles(t, dir), 1, "the new one is kept")

	// The volume is deleted, so its cache is
	require.NoError(t, c.stored("media", ""))
	assert.Empty(t, cacheFiles(t, dir))

	// A cache which isn't persistent is in memory
	m, release, _, err := c.acquire(ctx, "check", "")
	require.NoError(t, err)
	putCache(t, m, "k", "v")
	require.NoError(t, release())
	assert.Empty(t, cacheFiles(t, dir))

	// A restart deletes the caches of volumes which aren't stored, or of
	// their URLs which aren't, but not the others, nor what isn't a cache
	for _, name := range []string{"gone.000000000000.sqlite", "gone.000000000000.sqlite-wal", "kept.000000000000.sqlite", "notes.txt", "notes.sqlite", "notes.sqlite.backup", "kept.backup.sqlite"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0o600))
	}
	kept := filepath.Base(c.path("kept", "file://kept/x"))
	require.NoError(t, os.WriteFile(filepath.Join(dir, kept), nil, 0o600))
	require.NoError(t, c.sweep(map[string]string{"kept": "file://kept/x"}))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	slices.Sort(names)
	assert.Equal(t, []string{".lock", kept, "kept.backup.sqlite", "notes.sqlite", "notes.sqlite.backup", "notes.txt"}, names, "only caches, of their names, are deleted")
}

func Test_CachesPrivate(t *testing.T) {
	ctx := t.Context()
	dir := filepath.Join(t.TempDir(), "cache")
	require.NoError(t, os.Mkdir(dir, 0o755))
	require.NoError(t, os.Chmod(dir, 0o755))

	// A directory which is there is made only its owner's
	c, err := newCaches(dir)
	require.NoError(t, err)
	t.Cleanup(func() { c.close() })
	if runtime.GOOS != "windows" {
		info, err := os.Stat(dir)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())

		// And is the registry's alone, while it's open
		_, err = newCaches(dir)
		assert.ErrorIs(t, err, gofiler.ErrConflict)
	}

	// A volume's name, which its cache's path is of, is an identifier
	for _, name := range []string{"../other", "a/b", ""} {
		_, _, _, err := c.acquire(ctx, name, "id")
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, name)
	}

	// A clone retains the cache, which is kept open, until both are released
	cache, release, retain, err := c.acquire(ctx, "media", "id")
	require.NoError(t, err)
	releaseClone := retain()
	require.NoError(t, release())
	putCache(t, cache, "k", "v")
	require.NoError(t, releaseClone())
	_, err = getCache(t, cache, "k")
	assert.Error(t, err, "it's closed, once both are released")

	// A volume which is created again, of the same URL, has another cache,
	// so it starts again
	created := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	assert.NotEqual(t, c.path("media", cacheID("file://media/a", created)), c.path("media", cacheID("file://media/a", created.Add(time.Second))))
	assert.Equal(t, c.path("media", cacheID("file://media/a", created)), c.path("media", cacheID("file://media/a", created.In(time.FixedZone("x", 3600)))), "of its time, in any zone")

	// Once it's closed, another can lock it
	require.NoError(t, c.close())
	c2, err := newCaches(dir)
	require.NoError(t, err)
	require.NoError(t, c2.close())
}

func Test_CachesTemporary(t *testing.T) {
	c, err := newCaches("")
	require.NoError(t, err)
	dir := c.dir
	_, err = os.Stat(dir)
	require.NoError(t, err)
	require.NoError(t, c.close())
	_, err = os.Stat(dir)
	assert.ErrorIs(t, err, os.ErrNotExist, "it's removed once it's closed")
}

// Test_Registry_Cache tests a volume's backend is given its cache, which is
// kept after a restart, and starts again when its URL changes, and is
// deleted with it
func Test_Registry_Cache(t *testing.T) {
	dir, cacheDir := t.TempDir(), t.TempDir()
	s := newStore(t)
	var mu sync.Mutex
	var caches []store.CacheStore
	open := func(ctx context.Context, u *url.URL, cache store.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
		mu.Lock()
		caches = append(caches, cache)
		mu.Unlock()
		b, err := fsbackend.New(VolumeName(u), dir, nil, opts)
		if err != nil {
			return nil, err
		}
		return example{Filer: b, u: u}, nil
	}
	last := func() store.CacheStore {
		mu.Lock()
		defer mu.Unlock()
		require.NotEmpty(t, caches)
		return caches[len(caches)-1]
	}
	newCached := func() *Registry {
		r, err := New(context.Background(), WithDebounce(0), WithStore(s), WithCacheDir(cacheDir), WithBackend("cached", "Cached", open))
		require.NoError(t, err)
		runRegistry(t, r)
		return r
	}

	// A volume's backend is given its cache, which is a file, and the one
	// which checks its URL, when it's created, is given one in memory
	r := newCached()
	u, err := url.Parse("cached://notes")
	require.NoError(t, err)
	_, err = add(t, r, "notes", u)
	require.NoError(t, err)
	putCache(t, last(), "k", "before")
	assert.Len(t, cacheFiles(t, cacheDir), 1)

	// Its URL changes, so it starts again, and the old one is deleted
	_, err = update(t, r, "notes", url.Values{"description": {"Notes"}})
	require.NoError(t, err)
	_, err = getCache(t, last(), "k")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	putCache(t, last(), "k", "after")
	require.Eventually(t, func() bool { return len(cacheFiles(t, cacheDir)) == 1 }, 5*time.Second, 10*time.Millisecond)

	// After a restart, it's kept
	require.NoError(t, r.close())
	r = newCached()
	t.Cleanup(func() { r.close() })
	_, err = mounted(t, r, "notes", nil)
	require.NoError(t, err)
	v, err := getCache(t, last(), "k")
	require.NoError(t, err)
	assert.Equal(t, "after", v)

	// It's deleted with its volume, and one which is created again, of the
	// same URL, starts again
	require.NoError(t, del(t, r, "notes"))
	require.Eventually(t, func() bool { return len(cacheFiles(t, cacheDir)) == 0 }, 5*time.Second, 10*time.Millisecond)
	_, err = add(t, r, "notes", u)
	require.NoError(t, err)
	_, err = getCache(t, last(), "k")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
}
