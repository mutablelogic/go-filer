package sqlite

import (
	"context"
	"errors"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	filer "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	schema "github.com/mutablelogic/go-filer/store/sqlite/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	sqlite "github.com/mutablelogic/go-sqlite"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// GetObject returns the object of the volume of the key, with its metadata, or
// ErrNotFound
func (self *Store) GetObject(ctx context.Context, volume, key string) (_ *filer.Object, err error) {
	ctx, end := self.span(ctx, "GetObject", attribute.String("arg.volume", volume), attribute.String("arg.key", key))
	defer func() { end(err) }()
	if err := store.ValidateKey(key); err != nil {
		return nil, err
	}
	var result schema.ObjectList
	if err := self.conn.Get(ctx, &result, schema.ObjectKey{Volume: volume, Key: key}); errors.Is(err, sqlite.ErrNotFound) {
		return nil, gofiler.ErrNotFound.Withf("%q isn't indexed in volume %q", key, volume)
	} else if err != nil {
		return nil, storeErr(err)
	}
	return result[0], nil
}

// PutObject stores the objects of the volume, replacing those of their keys, and
// their metadata, in one transaction, or returns ErrNotFound when the
// volume isn't stored
func (self *Store) PutObject(ctx context.Context, volume string, objects ...*filer.Object) (err error) {
	ctx, end := self.span(ctx, "PutObject", attribute.String("arg.volume", volume), attribute.Int("arg.objects", len(objects)))
	defer func() { end(err) }()
	return storeErr(self.conn.Bulk(ctx, func(conn sqlite.Conn) error {
		var exists schema.VolumeList
		if err := conn.Get(ctx, &exists, schema.VolumeName(volume)); err != nil {
			return volumeErr(err, volume)
		}
		for _, object := range objects {
			if err := store.ValidateKey(store.Key(object)); err != nil {
				return err
			}
			if err := conn.Insert(ctx, nil, schema.ObjectPut{Volume: volume, Object: object}); err != nil {
				return err
			}
		}
		return nil
	}))
}

// SetMetadata replaces the metadata of the object of the volume of the key, of
// the source, a task's kind, in one transaction, or returns ErrNotFound when
// the object isn't indexed, or ErrPreconditionFailed when it isn't of the
// attributes, which the task read, any more
func (self *Store) SetMetadata(ctx context.Context, volume, key, source string, of *filer.ObjectAttr, meta []filer.Meta, geo []filer.Geometry) (err error) {
	ctx, end := self.span(ctx, "SetMetadata", attribute.String("arg.volume", volume), attribute.String("arg.key", key), attribute.String("arg.source", source), attribute.Int("arg.meta", len(meta)), attribute.Int("arg.geo", len(geo)))
	defer func() { end(err) }()
	if err := store.ValidateKey(key); err != nil {
		return err
	} else if !types.IsIdentifier(source) || strings.ToLower(source) != source {
		return gofiler.ErrBadParameter.Withf("the source of metadata is a task's kind, such as metadata, not %q", source)
	}
	for _, shape := range geo {
		if err := shape.Validate(); err != nil {
			return gofiler.ErrBadParameter.Withf("%q: %v", key, err)
		}
	}
	return storeErr(self.conn.Bulk(ctx, func(conn sqlite.Conn) error {
		var result schema.ObjectList
		if err := conn.Get(ctx, &result, schema.ObjectKey{Volume: volume, Key: key}); errors.Is(err, sqlite.ErrNotFound) {
			return gofiler.ErrNotFound.Withf("%q isn't indexed in volume %q", key, volume)
		} else if err != nil {
			return err
		} else if of != nil && !sameAttr(result[0].ObjectAttr, *of) {
			return gofiler.ErrPreconditionFailed.Withf("%q changed since it was read", key)
		}
		return conn.Insert(ctx, nil, schema.ObjectMetaPut{Volume: volume, Key: key, Source: source, Meta: meta, Geo: geo})
	}))
}

// DeleteMetadata removes the metadata of the source, a task's kind, of every
// object of the volume, and its task's work, and the revisions it was done of
func (self *Store) DeleteMetadata(ctx context.Context, volume, source string) (err error) {
	ctx, end := self.span(ctx, "DeleteMetadata", attribute.String("arg.volume", volume), attribute.String("arg.source", source))
	defer func() { end(err) }()
	if !types.IsIdentifier(source) || strings.ToLower(source) != source {
		return gofiler.ErrBadParameter.Withf("the source of metadata is a task's kind, such as metadata, not %q", source)
	}
	return storeErr(self.conn.Delete(ctx, nil, schema.ObjectMetaSource{Volume: volume, Source: source}))
}

// DeleteObject removes the object of the volume of the key, and, of a folder's,
// every object beneath it, with their metadata, which isn't an error when
// there's none
func (self *Store) DeleteObject(ctx context.Context, volume, key string) (err error) {
	ctx, end := self.span(ctx, "DeleteObject", attribute.String("arg.volume", volume), attribute.String("arg.key", key))
	defer func() { end(err) }()
	if err := store.ValidateKey(key); err != nil {
		return err
	}
	return storeErr(self.conn.Delete(ctx, nil, schema.ObjectKey{Volume: volume, Key: key}))
}

// ListObjects returns, in the order of their keys, up to the request's limit of
// the objects of a folder, or every object beneath it when it's recursive,
// after the key it starts after, without the folder
func (self *Store) ListObjects(ctx context.Context, volume string, req store.ListRequest) (_ []*filer.Object, err error) {
	ctx, end := self.span(ctx, "ListObjects", attribute.String("arg.volume", volume), attribute.String("arg.req", types.Stringify(req)))
	defer func() { end(err) }()
	if err := store.ValidateFolderKey(req.Parent); err != nil {
		return nil, err
	}
	result := schema.ObjectList{}
	if err := self.conn.List(ctx, &result, schema.ObjectListRequest{Volume: volume, ListRequest: req}); err != nil {
		return nil, storeErr(err)
	}
	return result, nil
}

// CountObjects returns how many objects a listing has, of every page, without the
// key it starts after, or its limit
func (self *Store) CountObjects(ctx context.Context, volume string, req store.ListRequest) (_ uint64, err error) {
	ctx, end := self.span(ctx, "CountObjects", attribute.String("arg.volume", volume), attribute.String("arg.req", types.Stringify(req)))
	defer func() { end(err) }()
	if err := store.ValidateFolderKey(req.Parent); err != nil {
		return 0, err
	}
	var count schema.ObjectCount
	if err := self.conn.Get(ctx, &count, schema.ObjectCountRequest{Volume: volume, ListRequest: req}); err != nil {
		return 0, storeErr(err)
	}
	return uint64(count), nil
}

// Revisions returns, by their keys, the revisions of the files of a folder's
// key, or of every file beneath it, when it's recursive
func (self *Store) Revisions(ctx context.Context, volume, folder string, recursive bool) (_ map[string]string, err error) {
	ctx, end := self.span(ctx, "Revisions", attribute.String("arg.volume", volume), attribute.String("arg.folder", folder), attribute.Bool("arg.recursive", recursive))
	defer func() { end(err) }()
	if err := store.ValidateFolderKey(folder); err != nil {
		return nil, err
	}
	result := schema.Revisions{}
	if err := self.conn.List(ctx, &result, schema.RevisionListRequest{Volume: volume, Folder: folder, Recursive: recursive}); err != nil {
		return nil, storeErr(err)
	}
	return result, nil
}

// SetRevisions stores the revisions of the files, by their keys, in one
// transaction, or returns ErrNotFound when the volume isn't stored
func (self *Store) SetRevisions(ctx context.Context, volume string, revisions map[string]string) (err error) {
	ctx, end := self.span(ctx, "SetRevisions", attribute.String("arg.volume", volume), attribute.Int("arg.revisions", len(revisions)))
	defer func() { end(err) }()
	if len(revisions) == 0 {
		return nil
	}
	now := time.Now()
	return storeErr(self.conn.Bulk(ctx, func(conn sqlite.Conn) error {
		var exists schema.VolumeList
		if err := conn.Get(ctx, &exists, schema.VolumeName(volume)); err != nil {
			return volumeErr(err, volume)
		}
		for key, revision := range revisions {
			if err := store.ValidateKey(key); err != nil {
				return err
			}
			if err := conn.Insert(ctx, nil, schema.RevisionPut{Volume: volume, Key: key, Revision: revision, Now: now}); err != nil {
				return err
			}
		}
		return nil
	}))
}

// sameAttr returns true when two versions of an object have the same
// attributes: entity tags, sizes, and when they were modified
func sameAttr(a, b filer.ObjectAttr) bool {
	sameETag := (a.ETag == nil) == (b.ETag == nil) && (a.ETag == nil || *a.ETag == *b.ETag)
	sameSize := (a.Size == nil) == (b.Size == nil) && (a.Size == nil || *a.Size == *b.Size)
	return sameETag && sameSize && a.ModTime.Equal(b.ModTime)
}
