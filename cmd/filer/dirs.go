package main

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"

	// Packages
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// dataDir returns the command's folder of the user's data, which is kept,
// unlike their cache, which can be cleared: its credentials, and its index.
// It's created, readable only by its owner. It's ~/Library/Application
// Support/<name> on macOS, %AppData%\<name> on Windows, and
// $XDG_DATA_HOME/<name>, or ~/.local/share/<name>, elsewhere.
func dataDir(ctx server.Cmd) (string, error) {
	var base string
	switch runtime.GOOS {
	case "darwin", "windows":
		dir, err := os.UserConfigDir()
		if err != nil {
			return "", err
		}
		base = dir
	default:
		if dir := os.Getenv("XDG_DATA_HOME"); filepath.IsAbs(dir) {
			base = dir
		} else if home, err := os.UserHomeDir(); err != nil {
			return "", err
		} else {
			base = filepath.Join(home, ".local", "share")
		}
	}
	dir := filepath.Join(base, ctx.Name())
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	return dir, nil
}

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
			return err
		}
	}
	return nil
}
