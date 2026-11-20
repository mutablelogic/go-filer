package sqlite

import (
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// newCache returns a cache in memory, which is closed when the test ends,
// whose time is the clock's
func newCache(t *testing.T) (*CacheStore, *time.Time) {
	t.Helper()
	cache, err := NewCacheStore(t.Context(), sqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close() })
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	cache.now = func() time.Time { return now }
	return cache, &now
}

// rows is a count of the rows of a table, of a literal query
type rows struct {
	table string
	n     int64
}

func (r *rows) Select(*sqlite.Bind, sqlite.Op) (string, error) {
	return "SELECT count(*) FROM " + r.table, nil
}

func (r *rows) Scan(row sqlite.Row) error {
	return row.Scan(&r.n)
}

// put stores an entry, which is closed
func put(t *testing.T, cache *CacheStore, key string, ttl time.Duration, meta store.CacheMeta, r io.Reader) {
	t.Helper()
	entry, err := cache.Put(t.Context(), key, ttl, meta, r)
	require.NoError(t, err)
	require.NoError(t, entry.Close())
}

// count returns how many rows a table of the cache has
func count(t *testing.T, cache *CacheStore, table string) int64 {
	t.Helper()
	r := &rows{table: table}
	require.NoError(t, cache.conn.Get(t.Context(), r, r))
	return r.n
}

// readAll returns the content of an entry, which is closed
func readAll(t *testing.T, entry store.CacheEntry) string {
	t.Helper()
	defer entry.Close()
	data, err := io.ReadAll(entry)
	require.NoError(t, err)
	return string(data)
}

// get returns the content of the entry of a key
func get(t *testing.T, cache *CacheStore, key string) string {
	t.Helper()
	entry, err := cache.Get(t.Context(), key)
	require.NoError(t, err)
	return readAll(t, entry)
}

func Test_NewCacheStore(t *testing.T) {
	_, err := NewCacheStore(t.Context(), " ")
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)

	// A file, which is kept, when it's opened again, without the entries
	// which were being written, or have expired
	path := filepath.Join(t.TempDir(), "media.sqlite")
	cache, err := NewCacheStore(t.Context(), path)
	require.NoError(t, err)
	put(t, cache, "kept", 0, store.CacheMeta{Version: "v1"}, strings.NewReader("kept"))
	put(t, cache, "brief", time.Millisecond, store.CacheMeta{}, strings.NewReader("brief"))
	require.NoError(t, cache.conn.Exec(t.Context(), `INSERT INTO cache_entry (stored) VALUES (0)`))
	assert.Equal(t, int64(3), count(t, cache, "cache_entry"))
	require.NoError(t, cache.Close())
	time.Sleep(5 * time.Millisecond)

	cache, err = NewCacheStore(t.Context(), path)
	require.NoError(t, err)
	defer cache.Close()
	assert.Equal(t, "kept", get(t, cache, "kept"))
	assert.Equal(t, int64(1), count(t, cache, "cache_entry"), "without the one being written, nor the one which expired")
	_, err = cache.Get(t.Context(), "brief")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
}

func Test_CachePutGet(t *testing.T) {
	ctx := t.Context()
	cache, now := newCache(t)

	// What isn't there, or of an empty key, or a negative TTL
	_, err := cache.Get(ctx, "missing")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	_, err = cache.Get(ctx, "")
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = cache.Put(ctx, "x", -time.Second, store.CacheMeta{}, strings.NewReader("x"))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)

	// An entry of more than one chunk, with what it is
	content := strings.Repeat("0123456789", 60_000) // 600,000 bytes, of three chunks
	meta := store.CacheMeta{Version: "v1", Values: map[string]string{"last-modified": "Fri, 20 Nov 2026 12:00:00 GMT"}}
	entry, err := cache.Put(ctx, "export/doc", time.Hour, meta, strings.NewReader(content))
	require.NoError(t, err)
	assert.Equal(t, meta, entry.Meta())
	assert.Equal(t, int64(len(content)), entry.Size())
	assert.Equal(t, *now, entry.Stored())
	assert.Equal(t, now.Add(time.Hour), entry.Expires())
	assert.Equal(t, content, readAll(t, entry))
	assert.Equal(t, int64(3), count(t, cache, "cache_chunk"))

	// It can seek, and be read at an offset, across its chunks
	entry, err = cache.Get(ctx, "export/doc")
	require.NoError(t, err)
	assert.Equal(t, meta, entry.Meta())
	buf := make([]byte, 10)
	n, err := entry.ReadAt(buf, cacheChunk-5)
	require.NoError(t, err)
	assert.Equal(t, content[cacheChunk-5:cacheChunk+5], string(buf[:n]))
	offset, err := entry.Seek(-4, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)-4), offset)
	rest, err := io.ReadAll(entry)
	require.NoError(t, err)
	assert.Equal(t, "6789", string(rest))
	n, err = entry.ReadAt(buf, int64(len(content)-3))
	assert.Equal(t, 3, n)
	assert.ErrorIs(t, err, io.EOF)
	require.NoError(t, entry.Close())
	_, err = entry.ReadAt(buf, 0)
	assert.Error(t, err, "once it's closed")

	// An empty entry, without a TTL
	entry, err = cache.Put(ctx, "empty", 0, store.CacheMeta{}, nil)
	require.NoError(t, err)
	assert.Zero(t, entry.Size())
	assert.True(t, entry.Expires().IsZero())
	assert.Equal(t, "", readAll(t, entry))

	// It expires, when it isn't returned, and is deleted
	*now = now.Add(2 * time.Hour)
	_, err = cache.Get(ctx, "export/doc")
	assert.ErrorIs(t, err, gofiler.ErrNotFound)
	assert.Equal(t, int64(0), count(t, cache, "cache_chunk"))
	assert.Equal(t, "", get(t, cache, "empty"), "but not one without a TTL")
}

func Test_CacheReplace(t *testing.T) {
	ctx := t.Context()
	cache, _ := newCache(t)
	put(t, cache, "tree", 0, store.CacheMeta{Version: "1"}, strings.NewReader("old"))

	// An entry which is read, when it's replaced, is read as it was, and is
	// deleted once it's closed
	old, err := cache.Get(ctx, "tree")
	require.NoError(t, err)
	put(t, cache, "tree", 0, store.CacheMeta{Version: "2"}, strings.NewReader("new"))
	assert.Equal(t, "new", get(t, cache, "tree"))
	assert.Equal(t, int64(2), count(t, cache, "cache_entry"))
	assert.Equal(t, "old", readAll(t, old))
	assert.Equal(t, int64(1), count(t, cache, "cache_entry"))

	// Content which can't be read keeps the entry, and isn't kept
	_, err = cache.Put(ctx, "tree", 0, store.CacheMeta{Version: "3"}, io.MultiReader(strings.NewReader("partial"), errReader{}))
	assert.ErrorIs(t, err, errBroken)
	entry, err := cache.Get(ctx, "tree")
	require.NoError(t, err)
	assert.Equal(t, "2", entry.Meta().Version)
	assert.Equal(t, "new", readAll(t, entry))
	assert.Equal(t, int64(1), count(t, cache, "cache_entry"))
}

func Test_CacheDelete(t *testing.T) {
	ctx := t.Context()
	cache, _ := newCache(t)
	for _, key := range []string{"export/a", "export/b", "exports", "tree"} {
		put(t, cache, key, 0, store.CacheMeta{}, strings.NewReader(key))
	}

	// A key, and the keys beneath a prefix, but not one which only starts
	// with it, and nothing, which isn't an error
	require.NoError(t, cache.Delete(ctx, "tree"))
	require.NoError(t, cache.Delete(ctx, "export/"))
	require.NoError(t, cache.Delete(ctx, "missing"))
	for _, key := range []string{"export/a", "export/b", "tree"} {
		_, err := cache.Get(ctx, key)
		assert.ErrorIs(t, err, gofiler.ErrNotFound, key)
	}
	assert.Equal(t, "exports", get(t, cache, "exports"))
	assert.Equal(t, int64(1), count(t, cache, "cache_entry"))
	assert.ErrorIs(t, cache.Delete(ctx, ""), gofiler.ErrBadParameter)
}

func Test_CacheGetOrCreate(t *testing.T) {
	ctx := t.Context()
	cache, _ := newCache(t)
	var creates atomic.Int32
	create := func(content string) func(io.Writer) (store.CacheMeta, error) {
		return func(w io.Writer) (store.CacheMeta, error) {
			creates.Add(1)
			time.Sleep(20 * time.Millisecond)
			_, err := io.WriteString(w, content)
			return store.CacheMeta{Values: map[string]string{"type": "docx"}}, err
		}
	}

	// It's created once, however many ask for it at once, of the version
	var wg sync.WaitGroup
	results := make([]string, 4)
	for i := range results {
		wg.Go(func() {
			entry, err := cache.GetOrCreate(ctx, "export/d1", "7", time.Hour, create("seven"))
			require.NoError(t, err)
			assert.Equal(t, "7", entry.Meta().Version)
			assert.Equal(t, "docx", entry.Meta().Values["type"])
			results[i] = readAll(t, entry)
		})
	}
	wg.Wait()
	assert.Equal(t, []string{"seven", "seven", "seven", "seven"}, results)
	assert.Equal(t, int32(1), creates.Load())

	// It isn't created again, of the version, but is of another
	entry, err := cache.GetOrCreate(ctx, "export/d1", "7", time.Hour, create("again"))
	require.NoError(t, err)
	assert.Equal(t, "seven", readAll(t, entry))
	assert.Equal(t, int32(1), creates.Load())
	entry, err = cache.GetOrCreate(ctx, "export/d1", "8", time.Hour, create("eight"))
	require.NoError(t, err)
	assert.Equal(t, "eight", readAll(t, entry))
	assert.Equal(t, int32(2), creates.Load())

	// A creation which fails returns its error, and keeps the entry
	_, err = cache.GetOrCreate(ctx, "export/d1", "9", time.Hour, func(w io.Writer) (store.CacheMeta, error) {
		io.WriteString(w, "partial")
		return store.CacheMeta{}, errBroken
	})
	assert.ErrorIs(t, err, errBroken)
	entry, err = cache.Get(ctx, "export/d1")
	require.NoError(t, err)
	assert.Equal(t, "8", entry.Meta().Version)
	entry.Close()

	// As does one which panics
	_, err = cache.GetOrCreate(ctx, "export/d1", "10", time.Hour, func(w io.Writer) (store.CacheMeta, error) {
		panic("oops")
	})
	assert.ErrorIs(t, err, gofiler.ErrInternalServerError)
	assert.Equal(t, int64(1), count(t, cache, "cache_entry"))
}

func Test_CacheJSON(t *testing.T) {
	ctx := t.Context()
	cache, _ := newCache(t)
	type tree struct {
		Root  string   `json:"root"`
		Files []string `json:"files"`
	}
	require.NoError(t, store.PutCacheJSON(ctx, cache, "tree", 0, store.CacheMeta{Version: "token-1"}, tree{Root: "r", Files: []string{"a", "b"}}))
	var got tree
	meta, err := store.GetCacheJSON(ctx, cache, "tree", &got)
	require.NoError(t, err)
	assert.Equal(t, "token-1", meta.Version)
	assert.Equal(t, tree{Root: "r", Files: []string{"a", "b"}}, got)
	_, err = store.GetCacheJSON(ctx, cache, "missing", &got)
	assert.ErrorIs(t, err, gofiler.ErrNotFound)

	// And a large one, of more than one chunk
	files := make([]string, 50_000)
	for i := range files {
		files[i] = strings.Repeat("x", 10)
	}
	require.NoError(t, store.PutCacheJSON(ctx, cache, "big", 0, store.CacheMeta{}, tree{Files: files}))
	_, err = store.GetCacheJSON(ctx, cache, "big", &got)
	require.NoError(t, err)
	assert.Len(t, got.Files, 50_000)
}

// errBroken is the error of content which can't be read
var errBroken = errors.New("broken")

// errReader is content which can't be read
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errBroken }
