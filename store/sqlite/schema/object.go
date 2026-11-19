package schema

import (
	"encoding/json"
	"strings"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	geo "github.com/mutablelogic/go-metadata/pkg/geo"
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

// ObjectMetaPut is the metadata of an object of a volume, of a source, which
// replaces the source's
type ObjectMetaPut struct {
	Volume string
	Key    string
	Source string
	Meta   []filer.Meta
	Geo    []geo.Geometry
}

// geoPut is a shape which is put, with its box
type geoPut struct {
	Shape geo.Geometry `json:"shape"`
	geo.Bounds
}

// ObjectMetaSource selects the metadata of a source of a volume, and its
// task's state, which is deleted
type ObjectMetaSource struct {
	Volume string
	Source string
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

// ObjectCountRequest selects how many objects a listing has, of every page,
// without where it starts after, or its limit
type ObjectCountRequest struct {
	Volume string
	store.ListRequest
}

// ObjectCount is how many objects a listing has
type ObjectCount uint64

// RevisionListRequest selects the revisions of the files of a folder's key,
// or beneath it
type RevisionListRequest struct {
	Volume    string
	Folder    string
	Recursive bool
}

// RevisionPut is the revision of a file of a volume, which is put
type RevisionPut struct {
	Volume   string
	Key      string
	Revision string
	Now      time.Time
}

// Revisions is the revisions which are read, by their keys
type Revisions map[string]string

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
	bind.Set("where", listWhere(bind, req.ListRequest))
	if req.Limit > 0 {
		bind.Set("offsetlimit", "LIMIT "+bind.Set("limit", req.Limit))
	}
	return bind.Query("object.list"), nil
}

func (req ObjectCountRequest) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.Get {
		return "", sqlite.ErrNotImplemented.Withf("object count: %v", op)
	}
	bind.Set("volume", req.Volume)
	bind.Set("where", listWhere(bind, req.ListRequest))
	return bind.Query("object.count"), nil
}

// listWhere returns the condition of the objects of a listing, of a folder,
// or beneath it, and of a type, which binds its values
func listWhere(bind *sqlite.Bind, req store.ListRequest) string {
	var where string
	if req.Recursive {
		lower, upper := store.Subtree(req.Parent)
		where = "key > " + bind.Set("lower", lower) + " AND key < " + bind.Set("upper", upper)
	} else {
		where = "parent = " + bind.Set("parent", req.Parent)
	}

	// A media type is the object's, and a major type is the start of it,
	// which is a range of its index, and a subtype is the end of it, which
	// is its own, which are searched for, of their indexes, rather than
	// filtered, of the objects of the folder, or beneath it
	if t := store.MediaType(req.Type); strings.Contains(t, "/") {
		where += " AND rowid IN (SELECT rowid FROM object WHERE volume = @volume AND media_type = " + bind.Set("type", t) + ")"
	} else if t != "" {
		where += " AND rowid IN (SELECT rowid FROM object WHERE volume = @volume AND media_type >= " + bind.Set("major", t+"/") + " AND media_type < " + bind.Set("next", t+"0") +
			" UNION ALL SELECT rowid FROM object WHERE volume = @volume AND media_subtype = " + bind.Set("type", t) + ")"
	}
	return where
}

func (source ObjectMetaSource) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.Delete {
		return "", sqlite.ErrNotImplemented.Withf("metadata of a source: %v", op)
	}
	bind.Set("volume", source.Volume)
	bind.Set("source", source.Source)
	bind.Set("did", Did(store.Kind(source.Source)))
	return bind.Query("object.delete_meta"), nil
}

func (req RevisionListRequest) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.List {
		return "", sqlite.ErrNotImplemented.Withf("revision list: %v", op)
	}
	bind.Set("volume", req.Volume)
	lower, upper := store.Subtree(req.Folder)
	where := "key > " + bind.Set("lower", lower) + " AND key < " + bind.Set("upper", upper)
	if !req.Recursive {
		where += " AND instr(substr(key, length(" + bind.Set("folder", req.Folder) + ") + 1), '/') = 0"
	}
	bind.Set("where", where)
	return bind.Query("revision.list"), nil
}

////////////////////////////////////////////////////////////////////////////////
// WRITERS

// Insert binds the revision of a file, which replaces the one it has
func (put RevisionPut) Insert(bind *sqlite.Bind) (string, error) {
	bind.Set("volume", put.Volume)
	bind.Set("key", put.Key)
	bind.Set("revision", put.Revision)
	bind.Set("now", put.Now.UTC())
	return bind.Query("revision.put"), nil
}

func (put RevisionPut) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("a revision is put")
}

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

// Insert binds the metadata, as a JSON array, of its source, and returns the
// query which replaces the source's metadata of the object
func (put ObjectMetaPut) Insert(bind *sqlite.Bind) (string, error) {
	meta := []byte("[]")
	if len(put.Meta) > 0 {
		var err error
		if meta, err = json.Marshal(put.Meta); err != nil {
			return "", sqlite.ErrBadParameter.Withf("metadata of %q: %v", put.Key, err)
		}
	}
	shapes := make([]geoPut, 0, len(put.Geo))
	for _, shape := range put.Geo {
		shapes = append(shapes, geoPut{Shape: shape, Bounds: shape.Bounds()})
	}
	geo, err := json.Marshal(shapes)
	if err != nil {
		return "", sqlite.ErrBadParameter.Withf("shapes of %q: %v", put.Key, err)
	}
	bind.Set("volume", put.Volume)
	bind.Set("key", put.Key)
	bind.Set("source", put.Source)
	bind.Set("meta", string(meta))
	bind.Set("geo", string(geo))
	return bind.Query("object.set_meta"), nil
}

func (put ObjectMetaPut) Update(*sqlite.Bind) error {
	return sqlite.ErrNotImplemented.With("an object's metadata is put")
}

////////////////////////////////////////////////////////////////////////////////
// READERS

func (count *ObjectCount) Scan(row sqlite.Row) error {
	return row.Scan((*uint64)(count))
}

func (revisions *Revisions) Scan(row sqlite.Row) error {
	var key, revision string
	if err := row.Scan(&key, &revision); err != nil {
		return err
	}
	if *revisions == nil {
		*revisions = Revisions{}
	}
	(*revisions)[key] = revision
	return nil
}

func (list *ObjectList) Scan(row sqlite.Row) error {
	var object filer.Object
	var key, meta, geo string
	if err := row.Scan(&key, &object.ContentType, &object.Size, &object.ETag, &object.ModTime, &meta, &geo); err != nil {
		return err
	}
	object.Path, object.IsDir = store.PathOf(key)
	if err := json.Unmarshal([]byte(meta), &object.Meta); err != nil {
		return err
	} else if len(object.Meta) == 0 {
		object.Meta = nil
	}
	if err := json.Unmarshal([]byte(geo), &object.Geo); err != nil {
		return err
	} else if len(object.Geo) == 0 {
		object.Geo = nil
	}
	*list = append(*list, &object)
	return nil
}
