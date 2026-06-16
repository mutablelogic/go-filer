package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
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

type MetadataKey string

type MetadataMeta struct {
	Title    string       `json:"title,omitempty"`
	Type     string       `json:"media_type,omitempty"`
	Summary  *string      `json:"summary,omitempty"`
	Tags     []string     `json:"tags,omitempty"`
	Metadata []MetadataKV `json:"metadata,omitempty"`
}

type MetadataCreate struct {
	Filename   string    `json:"filename,omitempty"`
	Etag       string    `json:"etag,omitempty"`
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modified_at,omitzero"`
	MetadataMeta
}

type Metadata struct {
	Key       string    `json:"key,omitempty"`
	IndexedAt time.Time `json:"indexed_at,omitzero"`
	MetadataCreate
}

type MetadataKV struct {
	Metadata string          `json:"-"`
	Key      string          `json:"key,omitempty"`
	Value    json.RawMessage `json:"value,omitempty"`
}

type MetadataListRequest struct {
	Title *string `json:"title,omitempty" help:"filter by title"`
	Type  *string `json:"type,omitempty" help:"media type filter"`
	pg.OffsetLimit
}

type MetadataQueryRequest struct {
	Query string  `json:"query,omitempty" arg:"" required:"" help:"full text search query"`
	Type  *string `json:"type,omitempty" help:"media type filter"`
	pg.OffsetLimit
}

type MetadataList struct {
	MetadataListRequest
	Count uint64      `json:"count"`
	Body  []*Metadata `json:"body"`
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func AppendMetadataKV(metadata []MetadataKV, key string, value any) []MetadataKV {
	// Check for zero or nil value
	kv := reflect.ValueOf(value)
	if kv.IsZero() {
		return metadata
	}

	// Marshal the value to JSON
	b, err := json.Marshal(value)
	if err != nil {
		return metadata
	}
	return append(metadata, MetadataKV{
		Key:   key,
		Value: json.RawMessage(b),
	})
}

/*
func MetadataFromPath(path string, info os.FileInfo) (*MetadataCreate, error) {
	if info == nil {
		var err error
		info, err = os.Stat(path)
		if err != nil {
			return nil, err
		}
	}

	etag, err := mime.EtagForPath(path)
	if err != nil {
		return nil, err
	}

	mimeType, err := mime.TypeFromPath(path)
	if err != nil {
		return nil, err
	}

	mediaType, params, err := mime.ParseMediaType(mimeType)
	if err != nil {
		return nil, err
	}

	kvs := make([]MetadataKV, 0, len(params))
	for k, v := range params {
		kvs = append(kvs, MetadataKV{
			Key:   k,
			Value: json.RawMessage(strconv.Quote(v)),
		})
	}

	filename := filepath.Base(path)
	title := strings.TrimSuffix(filename, filepath.Ext(filename))
	return types.Ptr(MetadataCreate{
		Filename:   filename,
		Etag:       etag,
		Size:       info.Size(),
		ModifiedAt: info.ModTime(),
		MetadataMeta: MetadataMeta{
			Title:    title,
			Type:     mediaType,
			Metadata: kvs,
		},
	}), nil
}
*/

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (m MetadataMeta) String() string {
	return types.Stringify(m)
}

func (m Metadata) String() string {
	return types.Stringify(m)
}

func (m MetadataKV) String() string {
	return types.Stringify(m)
}

func (r MetadataListRequest) String() string {
	return types.Stringify(r)
}

func (r MetadataQueryRequest) String() string {
	return types.Stringify(r)
}

func (r MetadataList) String() string {
	return types.Stringify(r)
}

////////////////////////////////////////////////////////////////////////////////
// METADATA MAP

func (r MetadataMeta) MetaMap() map[string]any {
	m := make(map[string]any, len(r.Metadata))
	for _, kv := range r.Metadata {
		if len(kv.Value) == 0 {
			continue
		}

		var value any
		if err := json.Unmarshal(kv.Value, &value); err != nil {
			continue
		} else {
			m[kv.Key] = value
		}
	}
	if len(r.Tags) > 0 {
		m["tags"] = r.Tags
	}
	return m
}

////////////////////////////////////////////////////////////////////////////////
// TUI TABLE

func (r Metadata) Header() []string {
	return []string{"Filename", "Media Type", "Size", "Modified At", "Title", "Summary", "Meta"}
}

func (r Metadata) Width(col int) int {
	return 0
}

func (r Metadata) Cell(col int) string {
	switch col {
	case 0:
		return r.Filename
	case 1:
		return r.Type
	case 2:
		return fmt.Sprint(r.Size)
	case 3:
		return fmt.Sprint(r.ModifiedAt)
	case 4:
		return r.Title
	case 5:
		return types.Value(r.Summary)
	case 6:
		metamap := r.MetaMap()
		if len(metamap) == 0 {
			return ""
		}
		return types.Stringify(metamap)
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (m *Metadata) Scan(row pg.Row) error {
	return row.Scan(
		&m.Key,
		&m.Etag,
		&m.Filename,
		&m.Size,
		&m.ModifiedAt,
		&m.Title,
		&m.Type,
		&m.Summary,
		&m.Tags,
		&m.IndexedAt,
	)
}

func (m *MetadataKV) Scan(row pg.Row) error {
	var value []byte
	if err := row.Scan(
		&m.Metadata,
		&m.Key,
		&value,
	); err != nil {
		return err
	}

	if len(value) == 0 {
		m.Value = nil
	} else {
		m.Value = append(m.Value[:0], value...)
	}

	return nil
}

func (l *MetadataList) Scan(row pg.Row) error {
	var metadata Metadata
	var metadataJSON []byte
	if err := row.Scan(
		&metadata.Key,
		&metadata.Etag,
		&metadata.Filename,
		&metadata.Size,
		&metadata.ModifiedAt,
		&metadata.Title,
		&metadata.Type,
		&metadata.Summary,
		&metadata.Tags,
		&metadata.IndexedAt,
		&metadataJSON,
	); err != nil {
		return err
	}
	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &metadata.Metadata); err != nil {
			return err
		}
	}
	l.Body = append(l.Body, &metadata)
	return nil
}

func (l *MetadataList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (k MetadataKey) Select(bind *pg.Bind, op pg.Op) (string, error) {
	key := strings.TrimSpace(string(k))
	if key == "" {
		return "", httpresponse.ErrBadRequest.With("missing metadata key")
	}

	bind.Set("key", key)

	switch op {
	case pg.Get:
		return bind.Query("extractor.metadata_get"), nil
	case pg.Delete:
		return bind.Query("extractor.metadata_delete"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported MetadataKey operation %q", op)
	}
}

func (r *MetadataListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Del("where")

	// Title
	if title := strings.TrimSpace(types.Value(r.Title)); title != "" {
		bind.Append("where", `"title" ILIKE `+bind.Set("title", "%"+title+"%"))
	}

	// Type
	value := strings.TrimSpace(types.Value(r.Type))
	switch {
	case value == "":
		break
	case types.IsIdentifier(value):
		bind.Append("where", `m."media_type" LIKE `+bind.Set("media_type_prefix", value+"/%"))
	case strings.Contains(value, "/") && strings.Count(value, "/") == 1:
		parts := strings.SplitN(value, "/", 2)
		if types.IsIdentifier(parts[0]) && types.IsIdentifier(parts[1]) {
			bind.Append("where", `"media_type" = `+bind.Set("media_type", value))
			break
		}
		fallthrough
	default:
		return "", httpresponse.ErrBadRequest.With("invalid media type filter")
	}

	if where := bind.Join("where", " AND "); where != "" {
		bind.Set("where", "WHERE "+where)
	} else {
		bind.Set("where", "")
	}

	// Set offset and limit
	r.OffsetLimit.Bind(bind, MetadataListLimit)

	switch op {
	case pg.List:
		return bind.Query("extractor.metadata_list"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported MetadataListRequest operation %q", op)
	}
}

func (r *MetadataQueryRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Del("where")

	// Query
	query := strings.TrimSpace(r.Query)
	if query == "" {
		return "", httpresponse.ErrBadRequest.With("missing search query")
	}
	bind.Set("query", query)
	bind.Append("where", `m."tsv" @@ websearch_to_tsquery('simple', `+bind.Set("query", query)+`)`)

	// Type
	value := strings.TrimSpace(types.Value(r.Type))
	switch {
	case value == "":
		break
	case types.IsIdentifier(value):
		bind.Append("where", `m."media_type" LIKE `+bind.Set("media_type_prefix", value+"/%"))
	case strings.Contains(value, "/") && strings.Count(value, "/") == 1:
		parts := strings.SplitN(value, "/", 2)
		if types.IsIdentifier(parts[0]) && types.IsIdentifier(parts[1]) {
			bind.Append("where", `m."media_type" = `+bind.Set("media_type", value))
			break
		}
		fallthrough
	default:
		return "", httpresponse.ErrBadRequest.With("invalid media type filter")
	}

	if where := bind.Join("where", " AND "); where != "" {
		bind.Set("where", "WHERE "+where)
	} else {
		bind.Set("where", "")
	}

	// Set offset and limit
	r.OffsetLimit.Bind(bind, MetadataListLimit)

	switch op {
	case pg.List:
		return bind.Query("extractor.metadata_query"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported MetadataQueryRequest operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (m MetadataCreate) Insert(bind *pg.Bind) (string, error) {
	if !bind.Has("key") {
		return "", gofiler.ErrBadParameter.With("missing metadata key")
	}
	bind.Set("filename", strings.TrimSpace(m.Filename))
	bind.Set("etag", strings.TrimSpace(m.Etag))
	bind.Set("size", m.Size)
	bind.Set("modified_at", m.ModifiedAt)
	bind.Set("title", strings.TrimSpace(m.Title))
	bind.Set("media_type", strings.TrimSpace(m.Type))
	bind.Set("summary", m.Summary)

	tags := m.Tags
	if tags == nil {
		tags = []string{}
	}
	bind.Set("tags", tags)

	return bind.Query("extractor.metadata_insert"), nil
}

func (m MetadataKV) Insert(bind *pg.Bind) (string, error) {
	metadata := strings.TrimSpace(m.Metadata)
	if metadata != "" {
		bind.Set("metadata", metadata)
	} else if !bind.Has("metadata") {
		return "", gofiler.ErrBadParameter.With("missing metadata key")
	}

	if key := strings.TrimSpace(m.Key); key == "" {
		return "", gofiler.ErrBadParameter.With("missing metadata property key")
	} else {
		bind.Set("key", key)
	}

	if len(m.Value) == 0 {
		bind.Set("value", nil)
	} else {
		bind.Set("value", string(m.Value))
	}

	return bind.Query("extractor.metadata_kv_insert"), nil
}

func (m MetadataKV) Update(bind *pg.Bind) error {
	return gofiler.ErrBadParameter.With("metadata_kv update is not supported; use insert upsert")
}

func (m MetadataMeta) Update(bind *pg.Bind) error {
	bind.Del("patch")

	if title := strings.TrimSpace(m.Title); title != "" {
		bind.Append("patch", `"title" = `+bind.Set("title", title))
	}

	if mediaType := strings.TrimSpace(m.Type); mediaType != "" {
		bind.Append("patch", `"media_type" = `+bind.Set("media_type", mediaType))
	}

	if m.Summary != nil {
		bind.Append("patch", `"summary" = `+bind.Set("summary", m.Summary))
	}

	if m.Tags != nil {
		bind.Append("patch", `"tags" = `+bind.Set("tags", m.Tags))
	}

	if patch := bind.Join("patch", ", "); patch == "" {
		return gofiler.ErrBadParameter.With("no patch values")
	} else {
		bind.Set("patch", patch)
	}

	return nil
}
