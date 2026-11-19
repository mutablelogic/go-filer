package schema

import (
	"encoding/json"

	// Packages
	filer "github.com/mutablelogic/go-filer/filer/schema"
	store "github.com/mutablelogic/go-filer/filer/store"
	sqlite "github.com/mutablelogic/go-sqlite"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ObjectKey selects an object of a volume, of its key, which is got, or
// deleted, with every object beneath it, of a folder's
type ObjectKey struct {
	Volume string
	Key    string
}

// ObjectPut is an object of a volume, which is put, with its metadata
type ObjectPut struct {
	Volume string
	*filer.Object
}

// ObjectListRequest selects the objects of a folder, or every object
// beneath it, in the order of their keys
type ObjectListRequest struct {
	Volume string
	store.ListRequest
}

// ObjectList is the objects which are read, of a list, or a get, which reads
// one
type ObjectList []*filer.Object

////////////////////////////////////////////////////////////////////////////////
// SELECTORS

func (key ObjectKey) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	bind.Set("volume", key.Volume)
	bind.Set("key", key.Key)
	switch op {
	case sqlite.Get:
		return bind.Query("object.get"), nil
	case sqlite.Delete:
		_, folder := store.PathOf(key.Key)
		lower, upper := store.Subtree(key.Key)
		bind.Set("folder", folder)
		bind.Set("lower", lower)
		bind.Set("upper", upper)
		return bind.Query("object.delete"), nil
	}
	return "", sqlite.ErrNotImplemented.Withf("object: %v", op)
}

func (req ObjectListRequest) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.List {
		return "", sqlite.ErrNotImplemented.Withf("object list: %v", op)
	}
	bind.Set("volume", req.Volume)
	bind.Set("after", req.After)
	if req.Recursive {
		lower, upper := store.Subtree(req.Parent)
		bind.Set("where", "key > "+bind.Set("lower", lower)+" AND key < "+bind.Set("upper", upper))
	} else {
		bind.Set("where", "parent = "+bind.Set("parent", req.Parent))
	}
	if req.Limit > 0 {
		bind.Set("offsetlimit", "LIMIT "+bind.Set("limit", req.Limit))
	}
	return bind.Query("object.list"), nil
}

////////////////////////////////////////////////////////////////////////////////
// WRITERS

// Insert binds an object, and its metadata, as a JSON array, and returns
// the query which puts it, replacing the object at its path, and its
// metadata
func (put ObjectPut) Insert(bind *sqlite.Bind) (string, error) {
	meta := []byte("[]")
	if len(put.Meta) > 0 {
		var err error
		if meta, err = json.Marshal(put.Meta); err != nil {
			return "", sqlite.ErrBadParameter.Withf("metadata of %q: %v", put.Path, err)
		}
	}
	bind.Set("volume", put.Volume)
	bind.Set("key", store.Key(put.Object))
	bind.Set("type", put.ContentType)
	bind.Set("size", put.Size)
	bind.Set("etag", put.ETag)
	bind.Set("modified_at", put.ModTime)
	bind.Set("meta", string(meta))
	return bind.Query("object.put"), nil
}

func (put ObjectPut) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("an object is put")
}

////////////////////////////////////////////////////////////////////////////////
// READERS

func (list *ObjectList) Scan(row sqlite.Row) error {
	var object filer.Object
	var key, meta string
	if err := row.Scan(&key, &object.ContentType, &object.Size, &object.ETag, &object.ModTime, &meta); err != nil {
		return err
	}
	object.Path, object.IsDir = store.PathOf(key)
	if err := json.Unmarshal([]byte(meta), &object.Meta); err != nil {
		return err
	} else if len(object.Meta) == 0 {
		object.Meta = nil
	}
	*list = append(*list, &object)
	return nil
}
