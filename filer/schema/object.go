package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Meta is a string key-value map for user-defined object metadata.
// Keys should be lowercase for S3 compatibility, as S3 normalizes all
// metadata keys to lowercase.
type Meta struct {
	Key   string          `json:"key,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
}

// Object represents a single stored item returned by the API.
type Object struct {
	ObjectKey
	ObjectMeta
	ObjectAttr
}

// ObjectKey represents the unique identifier of an object, which consists of a volume and a path.
type ObjectKey struct {
	Volume string `json:"volume,omitempty"`
	Path   string `json:"path,omitempty"`
}

type ObjectTouch ObjectKey

// ObjectMeta represents the metadata of an object, which can be updated.
type ObjectMeta struct {
	ContentType string `json:"type,omitempty"`
	Meta        []Meta `json:"meta,omitempty"`
}

// ObjectAttr represents the attributes of an object, which are immutable and cannot be updated.
type ObjectAttr struct {
	Size    int64     `json:"size"`
	ETag    *string   `json:"etag,omitempty"`
	ModTime time.Time `json:"last-modified,omitzero"`
	IsDir   bool      `json:"dir,omitempty"`
}

// ObjectCreate represents the result of creating an object in the database as part of indexing
type ObjectCreate struct {
	ObjectKey
	ObjectMeta
	ObjectAttr
}

// Operations
type CreateObjectRequest struct {
	Body        io.Reader `json:"-"`
	IfNotExists bool      // if true, fail with ErrConflict when the object already exists
	ObjectMeta
}

type GetObjectRequest struct {
	Path string
}

type ReadObjectRequest struct {
	GetObjectRequest
}

type DeleteObjectRequest struct {
	GetObjectRequest
}

type DeleteObjectsRequest struct {
	Path      string `json:"path,omitempty"`
	Recursive bool   `json:"recursive,omitempty"` // if true, delete all objects recursively; if false, delete only immediate children
}

type DeleteObjectsResponse struct {
	Volume string   `json:"volume,omitempty"`
	Body   []Object `json:"body,omitempty"` // list of deleted objects
}

type ObjectListRequest struct {
	pg.OffsetLimit
	Volume    *string `json:"volume,omitempty"`    // optional volume name to filter by
	Path      *string `json:"path,omitempty"`      // optional path prefix within the backend
	Recursive bool    `json:"recursive,omitempty"` // if true, list all objects recursively; if false, list only immediate children
}

type ObjectList struct {
	ObjectListRequest
	Count int       `json:"count"`          // total number of matching objects, before offset/limit
	Body  []*Object `json:"body,omitempty"` // page of objects; nil when Limit==0 (count-only)
}

var (
	metaKeyLeadInvalid = regexp.MustCompile(`^[^A-Za-z_]+`)
	metaKeyBodyInvalid = regexp.MustCompile(`[^A-Za-z0-9_-]`)
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func AppendMeta(kv []Meta, key string, value any) []Meta {
	key = sanitizeMetaKey(key)
	if key == "" {
		return kv
	}

	// Ignore zero-valued values
	if value == nil {
		return kv
	}
	if reflect.ValueOf(value).IsZero() {
		return kv
	}

	// Marshal the value to JSON
	data, err := json.Marshal(value)
	if err != nil {
		return kv
	} else {
		// PostgreSQL jsonb rejects Unicode NUL (\u0000) in text values.
		data = bytes.ReplaceAll(data, []byte(`\u0000`), []byte(""))
	}

	return append(kv, Meta{
		Key:   key,
		Value: data,
	})
}

func sanitizeMetaKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}

	// Enforce CHECK key ~ '^[A-Za-z_][A-Za-z0-9_-]*$' by replacing invalid
	// characters with '_' and forcing the first character to be valid.
	key = metaKeyBodyInvalid.ReplaceAllString(key, "_")
	if key == "" {
		return ""
	}
	key = metaKeyLeadInvalid.ReplaceAllString(key, "_")
	if key == "" {
		return "_"
	}
	return key
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (o Object) String() string {
	return types.Stringify(o)
}

func (k ObjectKey) String() string {
	return types.Stringify(k)
}

func (r CreateObjectRequest) String() string {
	return types.Stringify(r)
}

func (r GetObjectRequest) String() string {
	return types.Stringify(r)
}

func (r ObjectListRequest) String() string {
	return types.Stringify(r)
}

func (r ObjectList) String() string {
	return types.Stringify(r)
}

func (r DeleteObjectRequest) String() string {
	return types.Stringify(r)
}

func (r DeleteObjectsRequest) String() string {
	return types.Stringify(r)
}

func (r DeleteObjectsResponse) String() string {
	return types.Stringify(r)
}

////////////////////////////////////////////////////////////////////////////////
// QUERY BUILDER

func (r ObjectListRequest) Query() url.Values {
	query := url.Values{}
	if r.Volume != nil {
		query.Set("volume", *r.Volume)
	}
	if r.Path != nil {
		query.Set("path", *r.Path)
	}
	if r.Recursive {
		query.Set("recursive", "true")
	}
	if r.Offset > 0 {
		query.Set("offset", types.Stringify(r.Offset))
	}
	if r.Limit != nil {
		query.Set("limit", types.Stringify(types.Value(r.Limit)))
	}
	return query
}

////////////////////////////////////////////////////////////////////////////////
// TABLE OUTPUT

func (r Object) Header() []string {
	return []string{"Volume", "Path", "Size", "Content Type", "ETag", "Modified", "Meta"}
}

func (r Object) Width(col int) int {
	return 0
}

func (r Object) Cell(col int) string {
	switch col {
	case 0:
		return r.Volume
	case 1:
		return r.Path
	case 2:
		return fmt.Sprint(r.Size)
	case 3:
		return r.ContentType
	case 4:
		if r.ETag == nil {
			return ""
		}
		return *r.ETag
	case 5:
		if r.ModTime.IsZero() {
			return ""
		}
		return r.ModTime.Format(time.RFC3339)
	case 6:
		if len(r.Meta) == 0 {
			return ""
		}
		metamap := make(map[string]json.RawMessage, len(r.Meta))
		for _, kv := range r.Meta {
			metamap[kv.Key] = kv.Value
		}
		data, err := json.MarshalIndent(metamap, "", "  ")
		if err != nil {
			return err.Error()
		}
		return string(data)
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (o ObjectCreate) Insert(bind *pg.Bind) (string, error) {
	if o.Volume == "" {
		return "", gofiler.ErrBadParameter.With("missing object volume")
	} else {
		bind.Set("volume", o.Volume)
	}
	if o.Path == "" {
		return "", gofiler.ErrBadParameter.With("missing object path")
	} else {
		bind.Set("path", o.Path)
	}
	if o.ModTime.IsZero() {
		return "", gofiler.ErrBadParameter.With("missing modified time")
	} else {
		bind.Set("modified_at", o.ModTime)
	}
	if o.Size < 0 {
		return "", gofiler.ErrBadParameter.With("invalid object size")
	} else {
		bind.Set("size", o.Size)
	}
	if contentType := strings.TrimSpace(o.ContentType); contentType == "" {
		return "", gofiler.ErrBadParameter.With("missing content type")
	} else {
		bind.Set("type", contentType)
	}

	// Return the query
	return bind.Query("filer.object_upsert"), nil
}

func (m Meta) Insert(bind *pg.Bind) (string, error) {
	if volume, ok := bind.Get("volume").(string); !ok || strings.TrimSpace(volume) == "" {
		return "", gofiler.ErrBadParameter.With("missing object volume")
	}
	if path, ok := bind.Get("path").(string); !ok || strings.TrimSpace(path) == "" {
		return "", gofiler.ErrBadParameter.With("missing object path")
	}
	if key := sanitizeMetaKey(m.Key); key == "" {
		return "", gofiler.ErrBadParameter.With("missing metadata key")
	} else {
		bind.Set("key", key)
	}
	bind.Set("value", m.Value)

	// Return the query
	return bind.Query("filer.meta_upsert"), nil
}

func (m Meta) Update(bind *pg.Bind) error {
	return gofiler.ErrBadParameter.With("meta update is not supported; use insert")
}

func (m ObjectMeta) Update(bind *pg.Bind) error {
	bind.Del("patch")

	if m.ContentType != "" {
		bind.Append("patch", `"type" = `+bind.Set("type", m.ContentType))
	}
	if m.Meta != nil {
		data, err := json.Marshal(m.Meta)
		if err != nil {
			return err
		}
		bind.Append("patch", `"meta" = `+bind.Set("meta", string(data)))
	}

	if patch := bind.Join("patch", ", "); patch == "" {
		return gofiler.ErrBadParameter.With("no patch values")
	} else {
		bind.Set("patch", patch)
	}

	// Return success
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (o *Object) Scan(row pg.Row) error {
	var meta []byte

	if err := row.Scan(
		&o.Volume,
		&o.Path,
		&o.Size,
		&o.ContentType,
		&o.ETag,
		&o.ModTime,
		&meta,
	); err != nil {
		return err
	}

	if len(meta) == 0 {
		o.Meta = nil
		return nil
	}

	if err := json.Unmarshal(meta, &o.Meta); err != nil {
		return err
	}

	return nil
}

func (m *Meta) Scan(row pg.Row) error {
	var (
		volume string
		path   string
	)
	return row.Scan(
		&volume,
		&path,
		&m.Key,
		&m.Value,
	)
}

func (l *ObjectList) Scan(row pg.Row) error {
	var object Object
	if err := object.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, &object)
	return nil
}

func (l *ObjectList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (k ObjectKey) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if k.Volume == "" {
		return "", httpresponse.ErrBadRequest.With("missing object volume")
	} else {
		bind.Set("volume", k.Volume)
	}
	if k.Path == "" {
		return "", httpresponse.ErrBadRequest.With("missing object path")
	} else {
		bind.Set("path", k.Path)
	}

	switch op {
	case pg.Get:
		return bind.Query("filer.object_get"), nil
	case pg.Delete:
		return bind.Query("filer.object_delete"), nil
	case pg.Update:
		return bind.Query("filer.object_patch"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported ObjectKey operation %q", op)
	}
}

func (k ObjectTouch) Select(bind *pg.Bind, op pg.Op) (string, error) {
	object := ObjectKey(k)
	if object.Volume == "" {
		return "", httpresponse.ErrBadRequest.With("missing object volume")
	} else {
		bind.Set("volume", object.Volume)
	}
	if object.Path == "" {
		return "", httpresponse.ErrBadRequest.With("missing object path")
	} else {
		bind.Set("path", object.Path)
	}

	switch op {
	case pg.Update:
		return bind.Query("filer.object_touch"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported ObjectTouch operation %q", op)
	}
}

func (r *ObjectListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Del("where")

	if volume := strings.TrimSpace(types.Value(r.Volume)); volume != "" {
		if !types.IsIdentifier(volume) {
			return "", httpresponse.ErrBadRequest.Withf("invalid volume name %q", volume)
		}
		bind.Append("where", `o."volume" = `+bind.Set("volume", volume))
	}

	if path := strings.TrimSpace(types.Value(r.Path)); path != "" {
		pathPrefix := path
		if !strings.HasSuffix(pathPrefix, "/") {
			pathPrefix += "/"
		}

		if r.Recursive {
			bind.Append("where", `(
	o."path" = `+bind.Set("path", path)+`
	OR
	o."path" LIKE `+bind.Set("path_like", pathPrefix+"%")+`
)`)
		} else {
			bind.Append("where", `(
	o."path" = `+bind.Set("path", path)+`
	OR (
		o."path" LIKE `+bind.Set("path_like", pathPrefix+"%")+`
	AND
		split_part(substring(o."path" from `+bind.Set("path_prefix_offset", len(pathPrefix)+1)+`), '/', 2) = ''
	)
)`)
		}
	}

	if where := bind.Join("where", " AND "); where != "" {
		bind.Set("where", "WHERE "+where)
	} else {
		bind.Set("where", "")
	}

	r.OffsetLimit.Bind(bind, ObjectListLimit)

	switch op {
	case pg.List:
		return bind.Query("filer.object_list"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported ObjectListRequest operation %q", op)
	}
}
