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
// knows, and whether it isn't accepted, which only the watch uses, and,
// which the filer is locked for, as its writes change them too, the root's
// id, the files, and folders, by their ids, as a change doesn't say where a
// file was, their tree, by their paths, the versions of those which are
// removed, so an older change doesn't restore them, the events of the
// filer's writes, which the watch hasn't sent, and the signal that there are
// some, and, while the files are listed again, what the filer writes, which
// is applied to them, once they are
type driveWatch struct {
	token     string
	stale     bool
	root      string
	files     map[string]httpclient.DriveFile
	tree      *driveTree
	gone      map[string]int64
	pending   []schema.Event
	signal    chan struct{}
	relisting bool
	during    []httpclient.DriveChange

	// Whether the files have changed since they were saved in the volume's
	// cache, and when they were, and the ids of the Docs, Sheets and Slides
	// which were removed, whose exports in the cache are deleted
	dirty   bool
	saved   time.Time
	exports []string
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
// again, nor when the files are to be listed again, when they're listed at
// the next interval, and the files it knows are kept until then. One watch
// of a filer is at a time, and another is ErrConflict. Returns
// ErrNotImplemented when the refresh interval is zero, and the error of
// listing them, such as a SignInError, when a sign-in is needed.
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
	w := &driveWatch{signal: make(chan struct{}, 1)}
	if err := self.watch(w); err != nil {
		return err
	}
	defer self.unwatch(w)

	// The files, and their token, which were saved in the volume's cache,
	// such as before a restart, are of what changed since, which is got,
	// and applied, before the rescan, so the files aren't listed again,
	// unless there are none, or the token isn't accepted
	// unless there are none, or the token isn't accepted. What the filer
	// writes meanwhile is kept, and applied to them, once they're known.
	self.mu.Lock()
	w.relisting, w.during = true, nil
	self.mu.Unlock()
	restored := self.restore(ctx, w)
	if restored {
		if _, err := self.changes(ctx, w); errors.Is(err, fs.ErrNotExist) || errors.Is(err, gofiler.ErrBadParameter) {
			// The token isn't accepted, so the files are listed again, or,
			// when they can't be, as Google is unavailable, those which were
			// saved are kept, and listed again at the next interval
			if err := self.relist(ctx, w); errors.Is(err, gofiler.ErrServiceUnavailable) {
				w.stale = true
			} else if err != nil {
				return err
			}
		} else if err != nil && !errors.Is(err, gofiler.ErrServiceUnavailable) {
			return err
		}
	} else if err := self.relist(ctx, w); err != nil {
		return err
	}
	self.applyDuring(w)
	self.dropExports(ctx, w)

	// They're saved, once they've changed, and when it stops, so a restart
	// carries on from where it was
	defer self.save(context.WithoutCancel(ctx), w, true)
	self.save(ctx, w, false)
	if err := send(schema.Rescan(self.name)); err != nil {
		return err
	}
	ticker := time.NewTicker(self.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.signal:
			// What the filer wrote, which its tree has already
			for _, event := range self.take(w) {
				if err := send(event); err != nil {
					return err
				}
			}
			self.save(ctx, w, false)
			self.dropExports(ctx, w)
			continue
		case <-ticker.C:
		}
		var events []schema.Event
		var err error
		if !w.stale {
			events, err = self.changes(ctx, w)
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, gofiler.ErrBadParameter) {
				// The token isn't accepted, so what changed since isn't known,
				// and the files are listed again
				w.stale, err = true, nil
			}
		}
		if w.stale && err == nil {
			if err = self.relist(ctx, w); err == nil {
				w.stale, events = false, []schema.Event{schema.Rescan(self.name)}
			}
		}
		switch {
		case errors.Is(err, gofiler.ErrServiceUnavailable):
			// It's polled, or listed, again at the next interval, with the
			// files it knows kept until then
			continue
		case err != nil:
			return err
		}
		for _, event := range events {
			if err := send(event); err != nil {
				return err
			}
		}
		self.save(ctx, w, false)
		self.dropExports(ctx, w)
	}
}

// removedExports keeps the ids of the Docs, Sheets and Slides the watch
// knows, which the files which replace them haven't, so their exports, in
// the volume's cache, are deleted, which the filer must be locked for
func (w *driveWatch) removedExports(files map[string]httpclient.DriveFile) {
	for id, file := range w.files {
		if _, exists := files[id]; !exists && isExport(&file) {
			w.exports = append(w.exports, id)
		}
	}
}

// dropExports deletes the exports, in the volume's cache, of the Docs,
// Sheets and Slides which were removed, which isn't an error when they
// can't be, as they expire
func (self *DriveFiler) dropExports(ctx context.Context, w *driveWatch) {
	self.mu.Lock()
	ids := w.exports
	w.exports = nil
	self.mu.Unlock()
	for _, id := range ids {
		_ = self.cache.Delete(ctx, driveExportKey+id)
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// relist gets the token of the changes from now, and then the files, so a
// change as they're listed is applied again, and the filer lists them. What
// the filer writes as they're listed, which they may not have, is applied to
// them, once they are. The watch isn't changed when they can't be got.
func (self *DriveFiler) relist(ctx context.Context, w *driveWatch) error {
	// What the filer wrote before, as it starts, is kept, too
	self.mu.Lock()
	if !w.relisting {
		w.relisting, w.during = true, nil
	}
	self.mu.Unlock()
	token, root, files, err := self.list(ctx)
	self.mu.Lock()
	defer self.mu.Unlock()
	if err != nil {
		// What the filer wrote is kept, for the files it knows, or for when
		// they're listed again
		return err
	}
	during := w.during
	w.relisting, w.during = false, nil
	byId := make(map[string]httpclient.DriveFile, len(files))
	for _, file := range files {
		byId[file.Id] = file
	}
	gone := make(map[string]int64)
	applyChanges(byId, gone, during)
	w.removedExports(byId)
	w.token, w.root, w.files, w.gone, w.dirty = token, root, byId, gone, true
	w.tree = newDriveTree(root, slices.Collect(maps.Values(byId)))
	return nil
}

// applyDuring applies what the filer wrote, as the watch started, to the
// files it knows, when it didn't list them again, which applied them, and
// stops keeping them
func (self *DriveFiler) applyDuring(w *driveWatch) {
	self.mu.Lock()
	defer self.mu.Unlock()
	during := w.during
	w.relisting, w.during = false, nil
	if len(during) > 0 && w.tree != nil {
		files := maps.Clone(w.files)
		applyChanges(files, w.gone, during)
		w.removedExports(files)
		w.files, w.tree, w.dirty = files, newDriveTree(w.root, slices.Collect(maps.Values(files))), true
	}
}

// list gets the token of the changes from now, and then the root's id, and
// the files
func (self *DriveFiler) list(ctx context.Context) (string, string, []httpclient.DriveFile, error) {
	token, err := self.client.StartPageToken(ctx)
	if err != nil {
		return "", "", nil, self.apiError("watch", "/", err)
	}
	root, err := self.client.Root(ctx)
	if err != nil {
		return "", "", nil, self.apiError("watch", "/", err)
	}
	files, err := self.client.Files(ctx)
	if err != nil {
		return "", "", nil, self.apiError("watch", "/", err)
	}
	return token, root, files, nil
}

// changes gets the changes since the token, and applies them to the files,
// and returns the changes to the objects, which the filer then lists
func (self *DriveFiler) changes(ctx context.Context, w *driveWatch) ([]schema.Event, error) {
	changes, next, err := self.client.Changes(ctx, w.token)
	if err != nil {
		return nil, self.apiError("watch", "/", err)
	}
	w.token = next
	// What the filer wrote, which was applied first, is sent first
	self.mu.Lock()
	defer self.mu.Unlock()
	events := append(w.pending, self.apply(w, changes)...)
	w.pending = nil
	return events, nil
}

// apply applies changes to the files of a watch, which the filer must be
// locked for, and returns the changes to the objects, of the tree before
// them, and after them, which the filer then lists (see applyChanges)
func (self *DriveFiler) apply(w *driveWatch, changes []httpclient.DriveChange) []schema.Event {
	if len(changes) == 0 {
		return nil
	}
	files := maps.Clone(w.files)
	applyChanges(files, w.gone, changes)
	w.removedExports(files)
	before, after := w.tree, newDriveTree(w.root, slices.Collect(maps.Values(files)))
	w.files, w.tree, w.dirty = files, after, true
	return self.diff(before, after)
}

// wrote applies what the filer wrote, the files, and folders, as they are
// now, which are binned, of those it deletes, to the files of the watch,
// when it's watched, and sends it the events, as they're written, so it
// doesn't send them again, when they're polled, and, while the files are
// listed again, applies them to those which are listed, once they are, as
// they may not have them, or, when it isn't watched, clears the files which
// are kept, so they're got again
func (self *DriveFiler) wrote(files ...*httpclient.DriveFile) {
	self.mu.Lock()
	defer self.mu.Unlock()
	w := self.live
	if w == nil {
		self.tree.clear()
		return
	}
	changes := make([]httpclient.DriveChange, 0, len(files))
	for _, file := range files {
		changes = append(changes, httpclient.DriveChange{FileId: file.Id, File: *file})
	}
	if w.relisting {
		w.during = append(w.during, changes...)
	}
	if w.tree == nil {
		self.tree.clear()
		return
	}
	if events := self.apply(w, changes); len(events) > 0 {
		w.pending = append(w.pending, events...)
		select {
		case w.signal <- struct{}{}:
		default:
		}
	}
}

// take returns the events of what the filer wrote, which the watch hasn't
// sent, and empties them
func (self *DriveFiler) take(w *driveWatch) []schema.Event {
	self.mu.Lock()
	defer self.mu.Unlock()
	events := w.pending
	w.pending = nil
	return events
}

// watch starts a watch, or returns ErrConflict, when the filer is watched
// already, as one watch keeps the files the filer lists
func (self *DriveFiler) watch(w *driveWatch) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.live != nil {
		return gofiler.ErrConflict.Withf("volume %q is already watched", self.name)
	}
	self.live = w
	return nil
}

// unwatch stops the filer listing the files of a watch, which has stopped,
// and clears those which were got before it, which may be older, so
// they're got again
func (self *DriveFiler) unwatch(w *driveWatch) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.live == w {
		self.live = nil
	}
	self.tree.clear()
}

// liveTree returns the tree of the watch, or nil, when it isn't watched, or
// hasn't listed the files yet
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
// as who it's shared with, changes the version of, is updated, as is a
// folder which is another, of the same name, but not one whose version
// changed, as it does with what's in it. A path which
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
		case isDir(now) && old.Id == now.Id:
			// A folder's version changes with what's in it, which is sent
		case old.Id != now.Id, old.Version != now.Version, old.MD5Checksum != now.MD5Checksum, old.HeadRevision != now.HeadRevision, old.MimeType != now.MimeType, !old.ModifiedTime.Equal(now.ModifiedTime):
			changed = append(changed, event(schema.EventTypeUpdated, p))
		}
	}
	byPath := func(a, b schema.Event) int { return strings.Compare(a.Path, b.Path) }
	slices.SortFunc(deleted, byPath)
	slices.SortFunc(changed, byPath)
	return append(deleted, changed...)
}

// applyChanges applies changes to files, by their ids. A file which is
// binned, or isn't the person's, is removed, as it isn't listed, and its
// version is kept, in gone, so a change which is older, such as one a poll
// got before the filer deleted it, doesn't restore it, nor does a change
// which is older than the file, such as one a poll got before the filer
// wrote it, change it, so what's changed, by the filer, or a poll, isn't
// changed, nor sent, again by the other.
func applyChanges(files map[string]httpclient.DriveFile, gone map[string]int64, changes []httpclient.DriveChange) {
	for _, change := range changes {
		id, file := change.FileId, change.File
		if file.Id != "" {
			version := file.VersionNumber()
			if current, exists := files[id]; exists && !file.NotOlder(&current) {
				continue
			} else if removed, exists := gone[id]; exists && version >= 0 && version <= removed {
				continue
			}
		}
		if change.Removed || file.Trashed || !file.OwnedByMe || file.Id == "" {
			delete(files, id)
			if version := file.VersionNumber(); file.Id != "" && version >= 0 {
				gone[id] = version
			}
		} else {
			files[id] = file
			delete(gone, id)
		}
	}
}
