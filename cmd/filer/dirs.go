package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// oldCacheDir returns the user's cache folder, where the command's
// credentials, and its index, were kept, before they were kept in its
// folder of the user's data, so they're moved
func oldCacheDir() (string, error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return cache, nil
}

// moveFrom moves a file, or a folder, from where it was to where it is,
// when it's there, and there's none where it is, which isn't an error when
// it isn't there. A database is moved with its write-ahead log's files.
func moveFrom(from, to string, suffixes ...string) error {
	if _, err := os.Stat(from); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	} else if _, err := os.Stat(to); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	for _, suffix := range append(suffixes, "") {
		if err := os.Rename(from+suffix, to+suffix); err != nil && !errors.Is(err, fs.ErrNotExist) {
			// It isn't carried on without, such as when they're on different
			// filesystems, as what's kept, such as the credentials, would be
			// made again, empty, where it is, and the old ones not moved
			return fmt.Errorf("%q can't be moved to %q, so move it there, and start again: %w", from+suffix, to+suffix, err)
		}
	}
	return nil
}
