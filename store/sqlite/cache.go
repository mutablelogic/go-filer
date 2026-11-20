package sqlite

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"sync"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	store "github.com/mutablelogic/go-filer/store"
	schema "github.com/mutablelogic/go-filer/store/sqlite/schema"
	sqlite "github.com/mutablelogic/go-sqlite"
	ncruces "github.com/mutablelogic/go-sqlite/ncruces"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// CacheStore is the cache of a volume, in a SQLite database of its own,
// whose entries' content is in chunks, so it's written as it's read, and
// read of the chunks which are read, rather than all of it, and isn't kept
// in memory. An entry which is replaced, or deleted, while it's read loses
// its key, so it isn't found, and is deleted once it's closed.
type CacheStore struct {
	conn sqlite.PoolConn // The pool's connection, with the cache's queries
	now  func() time.Time

	// The entries which are read, by their ids, and how many times, those
	// which have lost their keys, which are deleted once they aren't read,
	// and the keys which are being created
	mu       sync.Mutex
	readers  map[int64]int
	released map[int64]bool
	creating map[string]*cacheCreate

	// beforeCreate is called once a key is claimed, before it's got again,
	// of a test, which creates it meanwhile
	beforeCreate func()
}

// cacheCreate is a creation of a key's entry, which is closed once it's
// created, or failed, when those which wait for it get it, or create it
type cacheCreate struct {
	done chan struct{}
}

// cacheEntry is an entry which is read, of its chunks, the last of which
// is kept, from its offset
type cacheEntry struct {
	store  *CacheStore
	entry  schema.CacheEntry
	offset int64

	mu     sync.Mutex
	chunk  []byte
	at     int64 // The index of the chunk, or -1
	closed bool
}

var _ store.CacheStore = (*CacheStore)(nil)
var _ store.CacheEntry = (*cacheEntry)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// cacheChunk is how many bytes each chunk of an entry's content is, which is
// all that's in memory of it, as it's written, or read
const cacheChunk = 256 << 10

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewCacheStore returns the cache of a volume, of the path of its database
// file, such as /var/cache/filer/media.sqlite, or sqlite.Memory, which is
// created when it doesn't exist, with its schema. The entries which were
// being written, or had lost their keys, when it was last closed, and those
// which have expired, are deleted.
func NewCacheStore(ctx context.Context, path string) (*CacheStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, gofiler.ErrBadParameter.With("the SQLite cache needs its path")
	}
	pool, err := ncruces.Open(ctx, path)
	if err != nil {
		return nil, storeErr(err)
	}
	queries, err := schema.NewCacheQueries()
	if err != nil {
		return nil, errors.Join(err, pool.Close())
	} else if err := schema.CreateCache(ctx, pool); err != nil {
		return nil, errors.Join(storeErr(err), pool.Close())
	}
	self := &CacheStore{
		conn:     pool.WithQueries(queries).(sqlite.PoolConn),
		now:      time.Now,
		readers:  make(map[int64]int),
		released: make(map[int64]bool),
		creating: make(map[string]*cacheCreate),
	}
	if err := self.sweep(ctx); err != nil {
		return nil, errors.Join(err, self.Close())
	}
	return self, nil
}

// Close closes the database
func (self *CacheStore) Close() error {
	return self.conn.Close()
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Get returns the entry of the key, or ErrNotFound when there's none, or it
// expired, which is deleted. It's selected, and its reader registered, at
// once, so a Put, or Delete, which releases it, deletes it once it's closed.
func (self *CacheStore) Get(ctx context.Context, key string) (store.CacheEntry, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	self.mu.Lock()
	var entry schema.CacheEntry
	if err := self.conn.Get(ctx, &entry, schema.CacheKey(key)); errors.Is(err, sqlite.ErrNotFound) {
		self.mu.Unlock()
		return nil, notFound(key)
	} else if err != nil {
		self.mu.Unlock()
		return nil, storeErr(err)
	} else if !entry.Expires.IsZero() && !self.now().Before(entry.Expires) {
		// It's released of its id, so one which has replaced it isn't
		self.mu.Unlock()
		if err := self.release(ctx, schema.CacheId(entry.Id)); err != nil {
			return nil, err
		}
		return nil, notFound(key)
	}
	self.readers[entry.Id]++
	self.mu.Unlock()
	return &cacheEntry{store: self, entry: entry, at: -1}, nil
}

// Put stores the content, as it's read, in chunks, as an entry without a
// key, which is given the key once it's all read, in place of the entry of
// the key, which is deleted, once it isn't read
func (self *CacheStore) Put(ctx context.Context, key string, ttl time.Duration, meta store.CacheMeta, r io.Reader) (store.CacheEntry, error) {
	return self.put(ctx, key, ttl, func() store.CacheMeta { return meta }, r)
}

// put is Put, whose metadata is got once its content is read, when it's
// committed
func (self *CacheStore) put(ctx context.Context, key string, ttl time.Duration, meta func() store.CacheMeta, r io.Reader) (store.CacheEntry, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	} else if ttl < 0 {
		return nil, gofiler.ErrBadParameter.Withf("cache %q: the TTL is negative", key)
	} else if r == nil {
		r = strings.NewReader("")
	}

	// An entry without a key, which its content is written to, which is
	// deleted, when it can't be
	var ids schema.CacheIds
	if err := self.conn.Insert(ctx, &ids, schema.CacheCreate{Now: self.now()}); err != nil {
		return nil, storeErr(err)
	}
	// It's read, so it isn't deleted, until the entry which is returned is
	// closed, which a Put, or Delete, which releases it once it's committed,
	// before it's returned, doesn't
	id := ids[0]
	self.mu.Lock()
	self.readers[id]++
	self.mu.Unlock()
	size, err := self.write(ctx, id, r)
	if err != nil {
		return nil, errors.Join(err, self.abandon(context.WithoutCancel(ctx), id))
	}

	// It's given the key, in place of the entry of the key
	now := self.now()
	commit := schema.CacheCommit{Id: id, Key: key, Meta: meta(), Size: size, Now: now}
	if ttl > 0 {
		commit.Expires = now.Add(ttl)
	}
	var entry schema.CacheEntry
	var released schema.CacheIds
	if err := self.conn.Tx(ctx, func(conn sqlite.Conn) error {
		if err := conn.Update(ctx, &released, schema.CacheKey(key), nil); err != nil && !errors.Is(err, sqlite.ErrNotFound) {
			return err
		}
		return conn.Update(ctx, &entry, commit, commit)
	}); err != nil {
		return nil, errors.Join(storeErr(err), self.abandon(context.WithoutCancel(ctx), id))
	}
	result := &cacheEntry{store: self, entry: entry, at: -1}
	if err := self.forget(ctx, released...); err != nil {
		return nil, errors.Join(err, result.Close())
	}
	return result, nil
}

// abandon deletes an entry which isn't written, and its reader, which wasn't
// returned
func (self *CacheStore) abandon(ctx context.Context, id int64) error {
	self.mu.Lock()
	if self.readers[id]--; self.readers[id] <= 0 {
		delete(self.readers, id)
		delete(self.released, id)
	}
	self.mu.Unlock()
	return self.delete(ctx, id)
}

// GetOrCreate returns the entry of the key, when there's one of the
// version, or else, once another which creates it is done, when there's
// one, or else stores what valuefn writes, as it's written
func (self *CacheStore) GetOrCreate(ctx context.Context, key, version string, ttl time.Duration, valuefn func(w io.Writer) (store.CacheMeta, error)) (store.CacheEntry, error) {
	for {
		entry, err := self.Get(ctx, key)
		if err == nil && entry.Meta().Version == version {
			return entry, nil
		} else if err == nil {
			entry.Close()
		} else if !errors.Is(err, gofiler.ErrNotFound) {
			return nil, err
		}

		// Another creation of the key is waited for, and then it's got again
		self.mu.Lock()
		if f, exists := self.creating[key]; exists {
			self.mu.Unlock()
			select {
			case <-f.done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		f := &cacheCreate{done: make(chan struct{})}
		self.creating[key] = f
		self.mu.Unlock()
		defer func() {
			self.mu.Lock()
			delete(self.creating, key)
			self.mu.Unlock()
			close(f.done)
		}()

		// Another may have created it, since it was got, and finished, before
		// this claimed it, so it's got again, now this has
		if self.beforeCreate != nil {
			self.beforeCreate()
		}
		if entry, err := self.Get(ctx, key); err == nil && entry.Meta().Version == version {
			return entry, nil
		} else if err == nil {
			entry.Close()
		} else if !errors.Is(err, gofiler.ErrNotFound) {
			return nil, err
		}
		return self.create(ctx, key, version, ttl, valuefn)
	}
}

// Delete deletes the entry of the key, or those beneath it, when it ends
// with "/", once they aren't read
func (self *CacheStore) Delete(ctx context.Context, key string) error {
	if err := checkKey(key); err != nil {
		return err
	} else if strings.HasSuffix(key, "/") {
		return self.release(ctx, schema.CachePrefix(key))
	}
	return self.release(ctx, schema.CacheKey(key))
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// create stores what valuefn writes, as it's written, through a pipe, with the
// metadata it returns, whose version is the version, when it hasn't one
func (self *CacheStore) create(ctx context.Context, key, version string, ttl time.Duration, valuefn func(w io.Writer) (store.CacheMeta, error)) (store.CacheEntry, error) {
	pr, pw := io.Pipe()
	var meta store.CacheMeta
	created := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			if r := recover(); r != nil {
				err = gofiler.ErrInternalServerError.Withf("cache %q: its value panicked: %v", key, r)
			}
			// Its error is wrapped, so one which is io.EOF isn't read as the
			// end of the content, which is stored
			if err != nil {
				pw.CloseWithError(valueError{err})
			} else {
				pw.Close()
			}
			created <- err
		}()
		meta, err = valuefn(pw)
	}()

	// The metadata is read once the content is, as valuefn returns it then,
	// which put commits after it's read all of it
	entry, err := self.put(ctx, key, ttl, func() store.CacheMeta {
		if meta.Version == "" {
			meta.Version = version
		}
		return meta
	}, pr)

	// When it isn't stored, valuefn's writes fail, so it returns, and its error
	// is returned, which is why the content couldn't be read, when it is
	pr.CloseWithError(err)
	ferr := <-created
	switch {
	case ferr != nil && entry != nil:
		entry.Close()
		return nil, ferr
	case ferr != nil && err != nil && !errors.Is(err, ferr):
		return nil, ferr
	case err != nil:
		return nil, err
	}
	return entry, nil
}

// write writes the content to an entry, in chunks, as it's read, and
// returns how many bytes it is. Only io.EOF of the reader is its end, so
// content which is cut short, such as of a response, whose reader returns
// io.ErrUnexpectedEOF, isn't stored.
func (self *CacheStore) write(ctx context.Context, id int64, r io.Reader) (int64, error) {
	var size int64
	buf := make([]byte, cacheChunk)
	for n := int64(0); ; n++ {
		read, err := readChunk(r, buf)
		if read > 0 {
			if err := self.conn.Insert(ctx, nil, schema.CacheChunk{Entry: id, N: n, Data: buf[:read]}); err != nil {
				return 0, storeErr(err)
			}
			size += int64(read)
		}
		if err == io.EOF {
			return size, nil
		} else if err != nil {
			return 0, err
		}
	}
}

// close closes an entry which was read, which is deleted, when it lost its
// key, and isn't read
func (self *CacheStore) close(id int64) error {
	self.mu.Lock()
	self.readers[id]--
	remove := self.readers[id] <= 0 && self.released[id]
	if self.readers[id] <= 0 {
		delete(self.readers, id)
		delete(self.released, id)
	}
	self.mu.Unlock()
	if remove {
		return self.delete(context.Background(), id)
	}
	return nil
}

// release takes the key of the entries the selector selects, so they aren't
// found, and deletes those which aren't read, or once they aren't
func (self *CacheStore) release(ctx context.Context, sel sqlite.Selector) error {
	var ids schema.CacheIds
	if err := self.conn.Update(ctx, &ids, sel, nil); err != nil && !errors.Is(err, sqlite.ErrNotFound) {
		return storeErr(err)
	}
	return self.forget(ctx, ids...)
}

// forget deletes the entries which have lost their keys, which aren't read,
// or once they aren't
func (self *CacheStore) forget(ctx context.Context, ids ...int64) error {
	var remove []int64
	self.mu.Lock()
	for _, id := range ids {
		if self.readers[id] > 0 {
			self.released[id] = true
		} else {
			remove = append(remove, id)
		}
	}
	self.mu.Unlock()
	var result error
	for _, id := range remove {
		result = errors.Join(result, self.delete(ctx, id))
	}
	return result
}

// delete deletes an entry, and its content, which isn't an error when
// there's none
func (self *CacheStore) delete(ctx context.Context, id int64) error {
	var ids schema.CacheIds
	if err := self.conn.Delete(ctx, &ids, schema.CacheId(id)); err != nil && !errors.Is(err, sqlite.ErrNotFound) {
		return storeErr(err)
	}
	return nil
}

// sweep deletes the entries without keys, which were being written, or
// had lost their keys, when it was last closed, and those which have
// expired, and reclaims their space
func (self *CacheStore) sweep(ctx context.Context) error {
	if err := self.release(ctx, schema.CacheExpired(self.now())); err != nil {
		return err
	}
	if err := self.conn.With(sqlite.TraceSpanNameArg, "cache.sweep").Exec(ctx, `DELETE FROM cache_entry WHERE key IS NULL`); err != nil {
		return storeErr(err)
	}
	if err := self.conn.With(sqlite.TraceSpanNameArg, "cache.vacuum").Exec(ctx, `PRAGMA incremental_vacuum`); err != nil {
		return storeErr(err)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// ENTRY

func (e *cacheEntry) Meta() store.CacheMeta {
	meta := e.entry.Meta
	if meta.Values != nil {
		values := make(map[string]string, len(meta.Values))
		for k, v := range meta.Values {
			values[k] = v
		}
		meta.Values = values
	}
	return meta
}

func (e *cacheEntry) Size() int64        { return e.entry.Size }
func (e *cacheEntry) Stored() time.Time  { return e.entry.Stored }
func (e *cacheEntry) Expires() time.Time { return e.entry.Expires }

// Read reads the content from the offset, of its chunks
func (e *cacheEntry) Read(p []byte) (int, error) {
	n, err := e.ReadAt(p, e.offset)
	e.offset += int64(n)
	if errors.Is(err, io.EOF) && n > 0 {
		err = nil
	}
	return n, err
}

// Seek changes the offset of the next read, from the start or end of the
// content, or the offset
func (e *cacheEntry) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += e.offset
	case io.SeekEnd:
		offset += e.entry.Size
	default:
		return 0, &fs.PathError{Op: "seek", Path: e.entry.Key, Err: fs.ErrInvalid}
	}
	if offset < 0 {
		return 0, &fs.PathError{Op: "seek", Path: e.entry.Key, Err: fs.ErrInvalid}
	}
	e.offset = offset
	return offset, nil
}

// ReadAt reads the content at the offset, of the chunks it's in, the last
// of which is kept, and is safe to call at once
func (e *cacheEntry) ReadAt(p []byte, off int64) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return 0, fs.ErrClosed
	} else if off < 0 {
		return 0, &fs.PathError{Op: "read", Path: e.entry.Key, Err: fs.ErrInvalid}
	}
	n := 0
	for n < len(p) && off < e.entry.Size {
		index := off / cacheChunk
		if index != e.at {
			var data schema.CacheData
			if err := e.store.conn.Get(context.Background(), &data, schema.CacheRead{Entry: e.entry.Id, N: index}); err != nil {
				return n, storeErr(err)
			}
			e.chunk, e.at = data, index
		}
		copied := copy(p[n:], e.chunk[off-index*cacheChunk:])
		if copied == 0 {
			return n, gofiler.ErrInternalServerError.Withf("cache %q: chunk %d is short", e.entry.Key, index)
		}
		n += copied
		off += int64(copied)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Close closes the entry, which is deleted, when it lost its key, and isn't
// read
func (e *cacheEntry) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed, e.chunk = true, nil
	e.mu.Unlock()
	return e.store.close(e.entry.Id)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// checkKey returns ErrBadParameter of a key which is empty
func checkKey(key string) error {
	if key == "" {
		return gofiler.ErrBadParameter.With("the cache's key is empty")
	}
	return nil
}

// notFound returns ErrNotFound of a key
func notFound(key string) error {
	return gofiler.ErrNotFound.Withf("cache %q", key)
}

// readChunk reads into the buffer, until it's full, or the reader returns
// an error, which is io.EOF at its end, and returns how many bytes it read
func readChunk(r io.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		read, err := r.Read(buf[n:])
		n += read
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// valueError is the error of a function which creates an entry's value,
// which isn't the end of its content, even when it's io.EOF
type valueError struct {
	error
}

func (e valueError) Unwrap() error {
	return e.error
}
