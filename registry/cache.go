package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	types "github.com/mutablelogic/go-server/pkg/types"
	gosqlite "github.com/mutablelogic/go-sqlite"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// caches are the caches of the volumes' backends, each a SQLite database in
// the directory, of the volume's name, and of the hash of its stored URL,
// and when it was created, such as media.1a2b3c4d5e6f.sqlite, so a volume
// which is mounted again, or after a restart, has the same cache, and one
// whose URL changes, or which is deleted, and created again, starts again.
// A cache is shared by the backends which are open of it, such as the one
// which replaces another, and their clones, and is closed once they're all
// closed, and deleted then, unless it's of the volume as it's stored. The
// directory is the registry's alone, while it's open, of a lock, so another
// server doesn't delete its caches.
type caches struct {
	dir  string
	temp bool     // The directory is removed, once the registry is closed
	lock *os.File // The lock of the directory, or nil, where it can't be locked

	mu      sync.Mutex
	open    map[string]*cacheRef // The caches which are open, by their paths
	current map[string]string    // The path of each volume's cache, as it's stored
}

// cacheRef is a cache which is open, and how many backends use it
type cacheRef struct {
	cache *sqlite.CacheStore
	refs  int
}

// cachedFiler is a backend, whose cache is released once it's closed, and
// which a clone of retains
type cachedFiler struct {
	gofiler.Filer
	retain  func() func() error
	release func() error
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// cacheExt is the extension of a cache's database, which its journal's
	// files have a suffix of
	cacheExt = ".sqlite"

	// cacheHash is how many bytes of the hash of a volume's identity are in
	// its cache's name
	cacheHash = 6

	// cacheLock is the name of the lock of the directory
	cacheLock = ".lock"
)

// reCache matches the name of a cache's database, of its volume, and the
// hash of its identity, and of its journal's files
var reCache = regexp.MustCompile(`^([A-Za-z0-9_-]+)\.[0-9a-f]{12}\.sqlite(-wal|-shm|-journal)?$`)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newCaches returns the caches, in the directory, which is created, or made,
// readable only by its owner, and locked, so it's the registry's alone, or,
// when it's empty, in a temporary directory, which is removed once they're
// closed. A directory which another registry has locked is ErrConflict.
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
	} else if runtime.GOOS != "windows" {
		if err := os.Chmod(dir, 0o700); err != nil {
			return nil, err
		}
	}
	lock, err := lockDir(filepath.Join(c.dir, cacheLock))
	if err != nil {
		return nil, errors.Join(err, c.close())
	}
	c.lock = lock
	return c, nil
}

// close unlocks the directory, and removes it, when it's temporary, once
// the backends, which have closed their caches, are closed
func (c *caches) close() error {
	if c == nil {
		return nil
	}
	var result error
	if c.lock != nil {
		result = unlockDir(c.lock)
		c.lock = nil
	}
	if c.temp {
		result = errors.Join(result, os.RemoveAll(c.dir))
	}
	return result
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// acquire returns the cache of a volume, of its identity (see cacheID),
// which is opened, unless it's open, and the function which releases it,
// which closes it, once no backend uses it, and deletes it, unless it's of
// the volume as it's stored, and the function which retains it again, of a
// clone. A cache whose identity is empty, such as that of a backend which is
// created to check a volume's URL, is in memory. A volume's name, which a
// cache's path is of, must be an identifier, so it's in the directory.
func (c *caches) acquire(ctx context.Context, volume, id string) (store.CacheStore, func() error, func() func() error, error) {
	if !types.IsIdentifier(volume) {
		return nil, nil, nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", volume)
	} else if id == "" {
		cache, err := sqlite.NewCacheStore(ctx, gosqlite.Memory)
		if err != nil {
			return nil, nil, nil, err
		}
		return cache, cache.Close, func() func() error { return func() error { return nil } }, nil
	}
	path := c.path(volume, id)
	c.mu.Lock()
	defer c.mu.Unlock()
	ref, exists := c.open[path]
	if !exists {
		cache, err := sqlite.NewCacheStore(ctx, path)
		if err != nil {
			return nil, nil, nil, err
		}
		ref = &cacheRef{cache: cache}
		c.open[path] = ref
	}
	ref.refs++
	return ref.cache, c.releaser(volume, path), func() func() error { return c.retain(volume, path) }, nil
}

// retain retains a cache which is open, of another backend, such as a
// clone, and returns the function which releases it
func (c *caches) retain(volume, path string) func() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ref := c.open[path]; ref != nil {
		ref.refs++
	}
	return c.releaser(volume, path)
}

// releaser returns the function which releases a cache, once
func (c *caches) releaser(volume, path string) func() error {
	var once sync.Once
	return func() (err error) {
		once.Do(func() { err = c.release(volume, path) })
		return err
	}
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

// stored sets the identity of a volume, as it's stored, so its cache is
// kept, once its backends are closed, and those of its others aren't, or,
// when it's empty, of a volume which is deleted, none of its caches are.
// Those which aren't open are deleted now.
func (c *caches) stored(volume, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id == "" {
		delete(c.current, volume)
	} else {
		c.current[volume] = c.path(volume, id)
	}
	return c.sweepLocked(func(name string) bool { return name == volume })
}

// sweep deletes the caches of the volumes which aren't stored, or of their
// identities which aren't, which aren't open, such as those left from
// before a restart, of the identities of the volumes, by their names
func (c *caches) sweep(volumes map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for volume, id := range volumes {
		c.current[volume] = c.path(volume, id)
	}
	return c.sweepLocked(func(string) bool { return true })
}

// sweepLocked deletes the caches, of the volumes it's true of, which
// aren't open, nor of a volume as it's stored, which must be locked. Only
// the files which are caches, of their names, are deleted.
func (c *caches) sweepLocked(of func(volume string) bool) error {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return err
	}
	var result error
	for _, entry := range entries {
		m := reCache.FindStringSubmatch(entry.Name())
		if m == nil || entry.IsDir() || !of(m[1]) {
			continue
		}
		path := filepath.Join(c.dir, entry.Name()[:len(entry.Name())-len(m[2])])
		if _, open := c.open[path]; open || c.current[m[1]] == path {
			continue
		}
		if err := os.Remove(filepath.Join(c.dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return result
}

// path returns the path of the cache of a volume, of its identity
func (c *caches) path(volume, id string) string {
	hash := sha256.Sum256([]byte(id))
	return filepath.Join(c.dir, volume+"."+hex.EncodeToString(hash[:cacheHash])+cacheExt)
}

////////////////////////////////////////////////////////////////////////////////
// BACKEND

// Close closes the backend, and then releases its cache
func (f *cachedFiler) Close() error {
	return errors.Join(closeFiler(f.Filer), f.release())
}

// Clone returns a clone of the backend, which retains its cache, which the
// clone shares, until it's closed
func (f *cachedFiler) Clone(ctx context.Context, patch url.Values) (gofiler.Filer, error) {
	release := f.retain()
	clone, err := f.Filer.Clone(ctx, patch)
	if err != nil {
		return nil, errors.Join(err, release())
	}
	return &cachedFiler{Filer: clone, retain: f.retain, release: release}, nil
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

// cacheID returns the identity of a volume's cache, as it's stored: its URL,
// and when it was created, so a volume which is deleted, and created again,
// of the same URL, starts again
func cacheID(volumeURL string, created time.Time) string {
	return volumeURL + "\n" + created.UTC().Format(time.RFC3339Nano)
}

// removeCache removes a cache's database, and its journal's files, which
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
