package schema

import (
	"context"
	_ "embed"
	"encoding/json"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	sqlite "github.com/mutablelogic/go-sqlite"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// CacheEntry is an entry of a volume's cache, as it's stored
type CacheEntry struct {
	Id      int64
	Key     string
	Meta    gofiler.CacheMeta
	Size    int64
	Stored  time.Time
	Expires time.Time
}

// CacheCreate is an entry, without a key, which its content is written to,
// whose id is returned
type CacheCreate struct {
	Now time.Time
}

// CacheChunk is a chunk of an entry's content, of its index
type CacheChunk struct {
	Entry int64
	N     int64
	Data  []byte
}

// CacheCommit is an entry, which is written, of its key, and what it is
type CacheCommit struct {
	Id      int64
	Key     string
	Meta    gofiler.CacheMeta
	Size    int64
	Now     time.Time
	Expires time.Time
}

// CacheKey selects the entry of a key, which is got, or released
type CacheKey string

// CachePrefix selects the entries of the keys which start with it, which
// are released
type CachePrefix string

// CacheExpired selects the entries whose TTLs have passed, at the time,
// which are released
type CacheExpired time.Time

// CacheId selects an entry, of its id, which is deleted
type CacheId int64

// CacheRead selects a chunk of an entry's content, of its index
type CacheRead struct {
	Entry int64
	N     int64
}

// CacheIds is the ids of the entries which are released, or deleted, or
// created
type CacheIds []int64

// CacheData is the data of a chunk which is read
type CacheData []byte

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// CacheVersion is the version of the cache's schema, of the database's
// user_version
const CacheVersion = 1

var (
	//go:embed cache_objects.sql
	cacheObjects string

	//go:embed cache_queries.sql
	cacheQueries string
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// CreateCache creates the cache's schema, when the database hasn't one,
// with its space reclaimed incrementally, and sets its version. A database
// whose schema is newer is an error.
func CreateCache(ctx context.Context, conn sqlite.Conn) error {
	objects, err := sqlite.NewQueries(strings.NewReader(cacheObjects))
	if err != nil {
		return err
	}

	// Space is reclaimed incrementally, which is set before there are any
	// tables, outside a transaction, and is kept once there are
	if err := conn.With(sqlite.TraceSpanNameArg, "cache.auto_vacuum").Exec(ctx, `PRAGMA auto_vacuum = INCREMENTAL`); err != nil {
		return err
	}
	return conn.Tx(ctx, func(conn sqlite.Conn) error {
		version, err := sqlite.Version(ctx, conn)
		if err != nil {
			return err
		} else if version > CacheVersion {
			return gofiler.ErrNotImplemented.Withf("the cache's schema version %d is newer than %d", version, CacheVersion)
		} else if version == CacheVersion {
			return nil
		}
		for _, key := range objects.Keys() {
			if err := conn.With(sqlite.TraceSpanNameArg, key).Exec(ctx, objects.Query(key)); err != nil {
				return err
			}
		}
		return sqlite.SetVersion(ctx, conn, CacheVersion)
	})
}

// NewCacheQueries returns the cache's queries, which a connection is bound
// to, with WithQueries, so the types' selectors and writers use them
func NewCacheQueries() (*sqlite.Queries, error) {
	return sqlite.NewQueries(strings.NewReader(cacheQueries))
}

////////////////////////////////////////////////////////////////////////////////
// WRITERS

func (create CacheCreate) Insert(bind *sqlite.Bind) (string, error) {
	bind.Set("now", create.Now.UnixNano())
	return bind.Query("cache.create"), nil
}

func (create CacheCreate) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("an entry is created")
}

func (chunk CacheChunk) Insert(bind *sqlite.Bind) (string, error) {
	bind.Set("entry", chunk.Entry)
	bind.Set("n", chunk.N)
	bind.Set("data", chunk.Data)
	return bind.Query("cache.write"), nil
}

func (chunk CacheChunk) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("a chunk is written")
}

func (commit CacheCommit) Insert(*sqlite.Bind) (string, error) {
	return "", sqlite.ErrNotImplemented.With("an entry is committed")
}

// Update binds an entry's key, and what it is, which its id selects
func (commit CacheCommit) Update(bind *sqlite.Bind) error {
	meta, err := json.Marshal(commit.Meta.Values)
	if err != nil {
		return err
	}
	bind.Set("id", commit.Id)
	bind.Set("key", commit.Key)
	bind.Set("version", commit.Meta.Version)
	bind.Set("meta", string(meta))
	bind.Set("size", commit.Size)
	bind.Set("now", commit.Now.UnixNano())
	if commit.Expires.IsZero() {
		bind.Set("expires", nil)
	} else {
		bind.Set("expires", commit.Expires.UnixNano())
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// SELECTORS

func (commit CacheCommit) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.Update {
		return "", sqlite.ErrNotImplemented.Withf("cache commit: %v", op)
	}
	return bind.Query("cache.commit"), nil
}

func (key CacheKey) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	bind.Set("key", string(key))
	switch op {
	case sqlite.Get:
		return bind.Query("cache.get"), nil
	case sqlite.Update:
		return bind.Query("cache.release"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("cache key: %v", op)
}

func (prefix CachePrefix) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.Update {
		return "", sqlite.ErrNotImplemented.Withf("cache prefix: %v", op)
	}
	bind.Set("prefix", string(prefix))
	return bind.Query("cache.release_prefix"), nil
}

func (expired CacheExpired) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.Update {
		return "", sqlite.ErrNotImplemented.Withf("cache expired: %v", op)
	}
	bind.Set("now", time.Time(expired).UnixNano())
	return bind.Query("cache.expired"), nil
}

func (id CacheId) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	bind.Set("id", int64(id))
	switch op {
	case sqlite.Delete:
		return bind.Query("cache.delete"), nil
	case sqlite.Update:
		return bind.Query("cache.release_id"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("cache id: %v", op)
}

func (read CacheRead) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.Get {
		return "", sqlite.ErrNotImplemented.Withf("cache read: %v", op)
	}
	bind.Set("entry", read.Entry)
	bind.Set("n", read.N)
	return bind.Query("cache.read"), nil
}

////////////////////////////////////////////////////////////////////////////////
// READERS

// Scan reads an entry
func (entry *CacheEntry) Scan(row sqlite.Row) error {
	var key *string
	var meta string
	var stored int64
	var expires *int64
	if err := row.Scan(&entry.Id, &key, &entry.Meta.Version, &meta, &entry.Size, &stored, &expires); err != nil {
		return err
	}
	entry.Key, entry.Meta.Values = "", nil
	if key != nil {
		entry.Key = *key
	}
	if meta != "" && meta != "null" && meta != "{}" {
		if err := json.Unmarshal([]byte(meta), &entry.Meta.Values); err != nil {
			return err
		}
	}
	entry.Stored, entry.Expires = time.Unix(0, stored).UTC(), time.Time{}
	if expires != nil {
		entry.Expires = time.Unix(0, *expires).UTC()
	}
	return nil
}

// Scan reads an id, of each row
func (ids *CacheIds) Scan(row sqlite.Row) error {
	var id int64
	if err := row.Scan(&id); err != nil {
		return err
	}
	*ids = append(*ids, id)
	return nil
}

// Scan reads a chunk's data
func (data *CacheData) Scan(row sqlite.Row) error {
	var b []byte
	if err := row.Scan(&b); err != nil {
		return err
	}
	*data = b
	return nil
}
