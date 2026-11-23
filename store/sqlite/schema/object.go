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
	filer.TaskMeta
}

// eventPut is an occurrence which is put, of its start, and end, as a date,
// or a time, which a row has one of
type eventPut struct {
	UID          string `json:"uid"`
	RecurrenceID string `json:"recurrence_id,omitempty"`
	Kind         string `json:"kind"`
	Title        string `json:"title,omitempty"`
	StartDate    string `json:"start_date,omitempty"`
	StartAt      string `json:"start_at,omitempty"`
	EndDate      string `json:"end_date,omitempty"`
	EndAt        string `json:"end_at,omitempty"`
	TZ           string `json:"tz,omitempty"`
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

// KindsRequest selects how many objects, without folders, of a volume are
// of each kind
type KindsRequest struct {
	Volume string
}

// Kinds is how many objects are of each kind, of their metadata, or else
// their media type, or "", of none
type Kinds map[string]uint64

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

func (req KindsRequest) Select(bind *sqlite.Bind, op sqlite.Op) (string, error) {
	if op != sqlite.List {
		return "", sqlite.ErrNotImplemented.Withf("kinds: %v", op)
	}
	bind.Set("volume", req.Volume)
	return bind.Query("object.kinds"), nil
}

// listWhere returns the condition of the objects of a listing, of a folder,
// or beneath it, of a type, and of objects only, without folders, which
// binds its values
func listWhere(bind *sqlite.Bind, req store.ListRequest) string {
	var where string
	if req.Recursive {
		lower, upper := store.Subtree(req.Parent)
		where = "key > " + bind.Set("lower", lower) + " AND key < " + bind.Set("upper", upper)
	} else {
		where = "parent = " + bind.Set("parent", req.Parent)
	}

	// A folder's key ends with a "/", whatever its type
	if req.Objects {
		where += " AND key NOT GLOB '*/'"
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
	occurrences := make([]eventPut, 0, len(put.Events))
	for _, o := range put.Events {
		row := eventPut{UID: o.UID, RecurrenceID: o.RecurrenceID, Kind: o.Kind, Title: o.Title, TZ: o.TZ}
		if o.Start.Date {
			row.StartDate = o.Start.String()
		} else {
			row.StartAt = o.Start.String()
		}
		if o.End != nil && o.End.Date {
			row.EndDate = o.End.String()
		} else if o.End != nil {
			row.EndAt = o.End.String()
		}
		occurrences = append(occurrences, row)
	}
	events, err := json.Marshal(occurrences)
	if err != nil {
		return "", sqlite.ErrBadParameter.Withf("events of %q: %v", put.Key, err)
	}
	bind.Set("volume", put.Volume)
	bind.Set("key", put.Key)
	bind.Set("source", put.Source)
	bind.Set("meta", string(meta))
	bind.Set("geo", string(geo))
	bind.Set("events", string(events))
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

func (kinds *Kinds) Scan(row sqlite.Row) error {
	var kind string
	var count uint64
	if err := row.Scan(&kind, &count); err != nil {
		return err
	}
	if *kinds == nil {
		*kinds = Kinds{}
	}
	(*kinds)[kind] = count
	return nil
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
	var key, meta, geo, events, artwork string
	if err := row.Scan(&key, &object.ContentType, &object.Size, &object.ETag, &object.ModTime, &meta, &geo, &events, &artwork); err != nil {
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
	if err := json.Unmarshal([]byte(events), &object.Events); err != nil {
		return err
	} else if len(object.Events) == 0 {
		object.Events = nil
	}
	if err := json.Unmarshal([]byte(artwork), &object.Artwork); err != nil {
		return err
	} else if len(object.Artwork) == 0 {
		object.Artwork = nil
	}
	*list = append(*list, &object)
	return nil
}
