package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	gosqlite "github.com/mutablelogic/go-sqlite"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// caches are the caches of the volumes' backends, each a SQLite database in
// the directory, of the volume's name, and its stored URL, such as
// media.1a2b3c4d5e6f.sqlite, so a volume which is mounted again, or after a
// restart, of the same URL, has the same cache, and one whose URL changes,
// or which is deleted, and created again, starts again. A cache is shared by
// the backends which are open of it, such as the one which replaces another,
// and is closed once they're all closed, and deleted then, unless it's of
// the volume as it's stored.
type caches struct {
	dir  string
	temp bool // The directory is removed, once the registry is closed

	mu      sync.Mutex
	open    map[string]*cacheRef // The caches which are open, by their paths
	current map[string]string    // The path of each volume's cache, of its stored URL
}

// cacheRef is a cache which is open, and how many backends use it
type cacheRef struct {
	cache *sqlite.CacheStore
	refs  int
}

// cachedFiler is a backend, whose cache is released once it's closed
type cachedFiler struct {
	gofiler.Filer
	release func() error
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// cacheExt is the extension of a cache's database, which its journal's
	// files, of its write-ahead log, have a suffix of
	cacheExt = ".sqlite"

	// cacheHash is how many bytes of the hash of a volume's URL are in its
	// cache's name
	cacheHash = 6
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newCaches returns the caches, in the directory, which is created, or, when
// it's empty, in a temporary directory, which is removed once they're closed
func newCaches(dir string) (*caches, error) {
	c := &caches{dir: dir, open: make(map[string]*cacheRef), current: make(map[string]string)}
	if dir == "" {
		temp, err := os.MkdirTemp("", "filer-cache-")
		if err != nil {
			return nil, err
		}
		c.dir, c.temp = temp, true
	} else if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return c, nil
}

// close removes the directory, when it's temporary, once the backends,
// which have closed their caches, are closed
func (c *caches) close() error {
	if c == nil || !c.temp {
		return nil
	}
	return os.RemoveAll(c.dir)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// acquire returns the cache of a volume, of its stored URL, which is opened,
// unless it's open, and the function which releases it, which closes it,
// once no backend uses it, and deletes it, unless it's of the volume as it's
// stored. A cache which isn't persistent, such as that of a backend which
// is created to check a volume's URL, is in memory.
func (c *caches) acquire(ctx context.Context, volume, volumeURL string, persistent bool) (store.CacheStore, func() error, error) {
	if !persistent {
		cache, err := sqlite.NewCacheStore(ctx, gosqlite.Memory)
		if err != nil {
			return nil, nil, err
		}
		return cache, cache.Close, nil
	}
	path := c.path(volume, volumeURL)
	c.mu.Lock()
	defer c.mu.Unlock()
	ref, exists := c.open[path]
	if !exists {
		cache, err := sqlite.NewCacheStore(ctx, path)
		if err != nil {
			return nil, nil, err
		}
		ref = &cacheRef{cache: cache}
		c.open[path] = ref
	}
	ref.refs++
	var once sync.Once
	return ref.cache, func() (err error) {
		once.Do(func() { err = c.release(volume, path) })
		return err
	}, nil
}

// release releases a backend's cache, which is closed, once no backend
// uses it, and deleted, unless it's of the volume as it's stored
func (c *caches) release(volume, path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	ref := c.open[path]
	if ref == nil {
		return nil
	} else if ref.refs--; ref.refs > 0 {
		return nil
	}
	delete(c.open, path)
	err := ref.cache.Close()
	if c.current[volume] != path {
		err = errors.Join(err, removeCache(path))
	}
	return err
}

// stored sets the stored URL of a volume, so its cache is kept, once its
// backends are closed, and those of its other URLs aren't, or, when it's
// empty, of a volume which is deleted, none of its caches are. Those which
// aren't open are deleted now.
func (c *caches) stored(volume, volumeURL string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if volumeURL == "" {
		delete(c.current, volume)
	} else {
		c.current[volume] = c.path(volume, volumeURL)
	}
	return c.sweepVolume(volume)
}

// sweep deletes the caches of the volumes which aren't stored, or of their
// URLs which aren't, which aren't open, such as those left from before a
// restart, of the stored URLs of the volumes, by their names
func (c *caches) sweep(volumes map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for volume, volumeURL := range volumes {
		c.current[volume] = c.path(volume, volumeURL)
	}
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	var result error
	for _, entry := range entries {
		path := filepath.Join(c.dir, cacheOf(entry.Name()))
		volume, _, ok := strings.Cut(entry.Name(), ".")
		if !ok || !strings.Contains(entry.Name(), cacheExt) {
			continue
		} else if _, open := c.open[path]; open || c.current[volume] == path {
			continue
		}
		result = errors.Join(result, removeCache(path))
	}
	return result
}

// sweepVolume deletes the caches of a volume which aren't open, nor of its
// stored URL, which must be locked
func (c *caches) sweepVolume(volume string) error {
	matches, err := filepath.Glob(filepath.Join(c.dir, volume+".*"+cacheExt))
	if err != nil {
		return err
	}
	var result error
	for _, path := range matches {
		if _, open := c.open[path]; open || c.current[volume] == path {
			continue
		}
		result = errors.Join(result, removeCache(path))
	}
	return result
}

// path returns the path of the cache of a volume, of its stored URL
func (c *caches) path(volume, volumeURL string) string {
	hash := sha256.Sum256([]byte(volumeURL))
	return filepath.Join(c.dir, volume+"."+hex.EncodeToString(hash[:cacheHash])+cacheExt)
}

////////////////////////////////////////////////////////////////////////////////
// BACKEND

// Close closes the backend, and then releases its cache
func (f *cachedFiler) Close() error {
	return errors.Join(closeFiler(f.Filer), f.release())
}

// Readonly returns true when the backend says its objects can't be written
func (f *cachedFiler) Readonly() bool {
	if b, ok := f.Filer.(interface{ Readonly() bool }); ok {
		return b.Readonly()
	}
	return false
}

// URLWithCredentials returns the backend's URL, with the placeholders of
// its credentials, when it has them, or its URL
func (f *cachedFiler) URLWithCredentials() *url.URL {
	if b, ok := f.Filer.(interface{ URLWithCredentials() *url.URL }); ok {
		return b.URLWithCredentials()
	}
	return f.Filer.URL()
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// removeCache removes a cache's database, and its write-ahead log, which
// isn't an error when they aren't there
func removeCache(path string) error {
	var result error
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return result
}

// cacheOf returns the name of a cache's database, of the name of one of its
// files, such as its write-ahead log's
func cacheOf(name string) string {
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if base, ok := strings.CutSuffix(name, suffix); ok {
			return base
		}
	}
	return name
}
