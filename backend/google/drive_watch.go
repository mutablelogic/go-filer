package google

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"slices"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// driveWatch is what a watch knows: the token of the changes after what it
// knows, the root's id, the files, and folders, by their ids, as a change
// doesn't say where a file was, and their tree, by their paths
type driveWatch struct {
	token string
	root  string
	files map[string]httpclient.DriveFile
	tree  *driveTree
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WatchObjects polls the changes to the files, at the refresh interval, and
// sends them, until the context is cancelled. Each change is applied to the
// files, and their paths are made again, so a path which is new is created,
// one whose file changed, such as its content, is updated, and one which is
// gone, such as when its file, or its folder, moved, was renamed, or was
// binned, is deleted. While it's watched, the filer lists, and reads, the
// files it knows, rather than getting them again.
//
// The first change is a rescan of the root, once the files are listed, of
// the token of the changes from before they were, so a change as they're
// listed is applied again, which changes nothing. When Google doesn't
// accept the token, such as one which has expired, the files are listed
// again, of another, and the root is rescanned. When Google is unavailable,
// it's polled again at the next interval, rather than listing the files
// again. Returns ErrNotImplemented when the refresh interval is zero, and
// the error of listing them, such as a SignInError, when a sign-in is
// needed.
// Reference: https://developers.google.com/drive/api/guides/manage-changes
func (self *DriveFiler) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	if self.refreshInterval == 0 {
		return gofiler.ErrNotImplemented.Withf("volume %q isn't watched, as its %s is zero", self.name, paramRefreshInterval)
	}
	send := func(event schema.Event) error {
		select {
		case ch <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w := &driveWatch{}
	defer self.unwatch(w)
	if err := self.relist(ctx, w); err != nil {
		return err
	} else if err := send(schema.Rescan(self.name)); err != nil {
		return err
	}
	ticker := time.NewTicker(self.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		events, err := self.changes(ctx, w)
		switch {
		case errors.Is(err, fs.ErrNotExist), errors.Is(err, gofiler.ErrBadParameter):
			// The token isn't accepted, so what changed since isn't known
			if err := self.relist(ctx, w); err != nil {
				return err
			}
			events = []schema.Event{schema.Rescan(self.name)}
		case errors.Is(err, gofiler.ErrServiceUnavailable):
			// It's polled again at the next interval
			continue
		case err != nil:
			return err
		}
		for _, event := range events {
			if err := send(event); err != nil {
				return err
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// relist gets the token of the changes from now, and then the files, so a
// change as they're listed is applied again, and the filer lists them
func (self *DriveFiler) relist(ctx context.Context, w *driveWatch) error {
	token, err := self.client.StartPageToken(ctx)
	if err != nil {
		return self.apiError("watch", "/", err)
	}
	root, err := self.client.Root(ctx)
	if err != nil {
		return self.apiError("watch", "/", err)
	}
	files, err := self.client.Files(ctx)
	if err != nil {
		return self.apiError("watch", "/", err)
	}
	w.token, w.root, w.files = token, root, make(map[string]httpclient.DriveFile, len(files))
	for _, file := range files {
		w.files[file.Id] = file
	}
	self.watched(w, newDriveTree(root, files))
	return nil
}

// changes gets the changes since the token, and applies them to the files,
// and returns the changes to the objects, of the tree before them, and
// after them, which the filer then lists. A file which is binned, or isn't
// the person's, is removed, as it isn't listed.
func (self *DriveFiler) changes(ctx context.Context, w *driveWatch) ([]schema.Event, error) {
	changes, next, err := self.client.Changes(ctx, w.token)
	if err != nil {
		return nil, self.apiError("watch", "/", err)
	} else if len(changes) == 0 {
		w.token = next
		return nil, nil
	}
	files := maps.Clone(w.files)
	for _, change := range changes {
		if change.Removed || change.File.Trashed || !change.File.OwnedByMe || change.File.Id == "" {
			delete(files, change.FileId)
		} else {
			files[change.File.Id] = change.File
		}
	}
	before, after := w.tree, newDriveTree(w.root, slices.Collect(maps.Values(files)))
	w.token, w.files = next, files
	self.watched(w, after)
	return self.diff(before, after), nil
}

// watched keeps the tree of a watch, which the filer lists, and reads, the
// files of, while it's watched
func (self *DriveFiler) watched(w *driveWatch, tree *driveTree) {
	self.mu.Lock()
	defer self.mu.Unlock()
	w.tree, self.live = tree, w
}

// unwatch stops the filer listing the files of a watch, which has stopped,
// so they're got again
func (self *DriveFiler) unwatch(w *driveWatch) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.live == w {
		self.live = nil
	}
}

// liveTree returns the tree of the watch, or nil, when it isn't watched
func (self *DriveFiler) liveTree() *driveTree {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.live == nil {
		return nil
	}
	return self.live.tree
}

// diff returns the changes from the tree before to the one after: a path
// which is gone is deleted, one which is new is created, and a file's whose
// id, version, content or type changed, which a change of its metadata, such
// as who it's shared with, changes the version of, is updated. A path which
// is a folder, and then a file, or a file, and then a folder, is deleted,
// and created. Those deleted are first, in the order of their paths, and
// then those created, or updated, so a folder is created before what's in
// it.
func (self *DriveFiler) diff(before, after *driveTree) []schema.Event {
	var deleted, changed []schema.Event
	event := func(t schema.EventType, p string) schema.Event {
		return schema.Event{EventType: t, ObjectKey: schema.ObjectKey{Name: self.name, Path: p}}
	}
	isDir := func(f *httpclient.DriveFile) bool { return f.MimeType == httpclient.DriveFolderType }
	for p, old := range before.files {
		if now, exists := after.files[p]; !exists || isDir(old) != isDir(now) {
			deleted = append(deleted, event(schema.EventTypeDeleted, p))
		}
	}
	for p, now := range after.files {
		old, exists := before.files[p]
		switch {
		case !exists, isDir(old) != isDir(now):
			changed = append(changed, event(schema.EventTypeCreated, p))
		case isDir(now):
		case old.Id != now.Id, old.Version != now.Version, old.MD5Checksum != now.MD5Checksum, old.HeadRevision != now.HeadRevision, old.MimeType != now.MimeType, !old.ModifiedTime.Equal(now.ModifiedTime):
			changed = append(changed, event(schema.EventTypeUpdated, p))
		}
	}
	byPath := func(a, b schema.Event) int { return strings.Compare(a.Path, b.Path) }
	slices.SortFunc(deleted, byPath)
	slices.SortFunc(changed, byPath)
	return append(deleted, changed...)
}
