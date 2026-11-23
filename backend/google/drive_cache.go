package google

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"slices"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// driveTreeKey is the prefix of the key of the files a watch knows, in
	// the volume's cache, of the id of the account's My Drive, so a clone of
	// another sign-in doesn't read, nor replace, another's, whose version is
	// the token of the changes after them, and driveTreeFormat the format of
	// its content, one file of JSON a line
	driveTreeKey    = "tree:"
	driveTreeFormat = "2"

	// The values of the entry: its format, the root's id, and the versions
	// of the files which were removed, as JSON
	driveTreeFormatValue = "format"
	driveTreeRootValue   = "root"
	driveTreeGoneValue   = "gone"
)

// driveSaveInterval is the least time between the saves of the files a
// watch knows, while they change, which is a variable, so a test can set it
var driveSaveInterval = 5 * time.Minute

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// restore sets the files a watch knows, the token of the changes after
// them, and the versions of those which were removed, to those which were
// saved in the volume's cache, of the account's My Drive, and returns true,
// or false, when there are none, or they can't be read, such as when
// they're of another format, or the account's My Drive can't be got
func (self *DriveFiler) restore(ctx context.Context, w *driveWatch) bool {
	root, err := self.client.Root(ctx)
	if err != nil || root == "" {
		return false
	}
	entry, err := self.cache.Get(ctx, driveTreeKey+root)
	if err != nil {
		return false
	}
	defer entry.Close()
	meta := entry.Meta()
	if meta.Values[driveTreeFormatValue] != driveTreeFormat || meta.Version == "" || meta.Values[driveTreeRootValue] != root {
		return false
	}
	gone := make(map[string]int64)
	if value := meta.Values[driveTreeGoneValue]; value != "" {
		if err := json.Unmarshal([]byte(value), &gone); err != nil {
			return false
		}
	}

	// The files, of a line each
	files := make(map[string]httpclient.DriveFile)
	dec := json.NewDecoder(entry)
	for {
		var file httpclient.DriveFile
		if err := dec.Decode(&file); errors.Is(err, io.EOF) {
			break
		} else if err != nil || file.Id == "" {
			return false
		}
		files[file.Id] = file
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	w.token, w.root, w.files, w.gone = meta.Version, root, files, gone
	w.tree = newDriveTree(root, slices.Collect(maps.Values(files)))
	return true
}

// save stores the files a watch knows, and the token of the changes after
// them, in the volume's cache, when they've changed since they were saved,
// and they weren't saved for driveSaveInterval, or now, when it's true. The
// files are written as they're encoded, through a pipe, so they aren't all
// in memory twice. A save which fails is tried again, after the interval,
// as the cache is never the only copy of them.
func (self *DriveFiler) save(ctx context.Context, w *driveWatch, now bool) {
	self.mu.Lock()
	if !w.dirty || w.tree == nil || (!now && time.Since(w.saved) < driveSaveInterval) {
		self.mu.Unlock()
		return
	}
	token, root := w.token, w.root
	files := slices.Collect(maps.Values(w.files))
	gone, err := json.Marshal(w.gone)
	w.dirty = false
	self.mu.Unlock()
	if err != nil {
		return
	}

	pr, pw := io.Pipe()
	go func() {
		enc := json.NewEncoder(pw)
		for i := range files {
			if err := enc.Encode(&files[i]); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		pw.Close()
	}()
	meta := gofiler.CacheMeta{Version: token, Values: map[string]string{driveTreeFormatValue: driveTreeFormat, driveTreeRootValue: root, driveTreeGoneValue: string(gone)}}
	entry, err := self.cache.Put(ctx, driveTreeKey+root, 0, meta, pr)
	pr.CloseWithError(err)
	self.mu.Lock()
	defer self.mu.Unlock()
	if err != nil {
		w.dirty = true
	} else {
		entry.Close()
	}
	w.saved = time.Now()
}
