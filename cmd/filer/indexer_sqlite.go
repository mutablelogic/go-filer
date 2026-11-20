//go:build !client && !indexer_memory && !indexer_pg

package main

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	// Packages
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	server "github.com/mutablelogic/go-server"
)

// IndexerFlags are the flags of the store of the indexes of volumes, which
// is a SQLite database, of make INDEXER_BACKEND=sqlite, which is the
// default, index.sqlite in the command's folder of the user's cache, beside
// its credentials, and the volumes' caches, which has none
type IndexerFlags struct{}

// indexFile is the name of the index's database, in the command's folder of
// the user's cache
const indexFile = "index.sqlite"

// store returns the store of the indexes of volumes, and the function
// which closes it. An index of an earlier version, <name>-index.db, in the
// user's cache folder, is moved to where it is now, when there's none there.
func (IndexerFlags) store(ctx server.Cmd, opts ...store.Opt) (store.Store, func(), error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return nil, nil, err
	}
	dir := filepath.Join(cache, ctx.Name())
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, indexFile)
	if err := moveIndex(filepath.Join(cache, ctx.Name()+"-index.db"), path); err != nil {
		return nil, nil, err
	}
	s, err := sqlite.New(ctx.Context(), path, opts...)
	if err != nil {
		return nil, nil, err
	}
	return s, func() { _ = s.Close() }, nil
}

// moveIndex moves the database of an index, and its write-ahead log, from
// where it was to where it is, when it's there, and there's none where it
// is, which isn't an error when it isn't there
func moveIndex(from, to string) error {
	if _, err := os.Stat(from); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	} else if _, err := os.Stat(to); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm", ""} {
		if err := os.Rename(from+suffix, to+suffix); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}
