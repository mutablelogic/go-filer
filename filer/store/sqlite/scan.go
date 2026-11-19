package sqlite

import (
	"context"
	"time"

	// Packages
	store "github.com/mutablelogic/go-filer/filer/store"
	schema "github.com/mutablelogic/go-filer/filer/store/sqlite/schema"
	sqlite "github.com/mutablelogic/go-sqlite"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Seen records the keys which a scan has seen, with the revisions of their
// listings, in one transaction, or returns ErrNotFound when the volume isn't
// stored
func (self *Store) Seen(ctx context.Context, volume string, seen map[string]string) (err error) {
	ctx, end := self.span(ctx, "Seen", attribute.String("arg.volume", volume), attribute.Int("arg.seen", len(seen)))
	defer func() { end(err) }()
	if len(seen) == 0 {
		return nil
	}
	now := time.Now()
	return storeErr(self.conn.Bulk(ctx, func(conn sqlite.Conn) error {
		var exists schema.VolumeList
		if err := conn.Get(ctx, &exists, schema.VolumeName(volume)); err != nil {
			return volumeErr(err, volume)
		}
		for key, revision := range seen {
			if err := store.ValidateKey(key); err != nil {
				return err
			}
			if err := conn.Insert(ctx, nil, schema.ScanSeen{Volume: volume, Key: key, Revision: revision, Now: now}); err != nil {
				return err
			}
		}
		return nil
	}))
}

// Unsee forgets what a scan has seen beneath the folder's key
func (self *Store) Unsee(ctx context.Context, volume, folder string) (err error) {
	ctx, end := self.span(ctx, "Unsee", attribute.String("arg.volume", volume), attribute.String("arg.folder", folder))
	defer func() { end(err) }()
	if err := store.ValidateFolderKey(folder); err != nil {
		return err
	}
	return storeErr(self.conn.Delete(ctx, nil, schema.ScanUnsee{Volume: volume, Folder: folder}))
}

// Sweep removes the objects beneath the folder's key, with their metadata,
// which a scan of it hasn't seen
func (self *Store) Sweep(ctx context.Context, volume, folder string) (err error) {
	ctx, end := self.span(ctx, "Sweep", attribute.String("arg.volume", volume), attribute.String("arg.folder", folder))
	defer func() { end(err) }()
	if err := store.ValidateFolderKey(folder); err != nil {
		return err
	}
	return storeErr(self.conn.Delete(ctx, nil, schema.ScanSweep{Volume: volume, Folder: folder}))
}

// Changed returns, in the order of their keys, after the key, up to the
// limit, the files beneath the folder's key which a scan of it has seen,
// which aren't indexed, or whose revisions aren't those they were stored of
func (self *Store) Changed(ctx context.Context, volume, folder, after string, limit int) (_ []store.Seen, err error) {
	ctx, end := self.span(ctx, "Changed", attribute.String("arg.volume", volume), attribute.String("arg.folder", folder), attribute.String("arg.after", after), attribute.Int("arg.limit", limit))
	defer func() { end(err) }()
	if err := store.ValidateFolderKey(folder); err != nil {
		return nil, err
	}
	result := schema.ScanChangedList{}
	if err := self.conn.List(ctx, &result, schema.ScanChanged{Volume: volume, Folder: folder, After: after, Limit: limit}); err != nil {
		return nil, storeErr(err)
	}
	return result, nil
}

// CountChanged returns how many files Changed returns, of every page
func (self *Store) CountChanged(ctx context.Context, volume, folder string) (_ uint64, err error) {
	ctx, end := self.span(ctx, "CountChanged", attribute.String("arg.volume", volume), attribute.String("arg.folder", folder))
	defer func() { end(err) }()
	if err := store.ValidateFolderKey(folder); err != nil {
		return 0, err
	}
	var count schema.ScanCount
	if err := self.conn.Get(ctx, &count, schema.ScanFolder{Volume: volume, Folder: folder}); err != nil {
		return 0, storeErr(err)
	}
	return uint64(count), nil
}
