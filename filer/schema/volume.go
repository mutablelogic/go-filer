package schema

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type VolumeName string
type VolumeTouch string

type VolumeMeta struct {
	Enabled    *bool          `json:"enabled,omitempty" negatable:""`
	IndexDelta *time.Duration `json:"delta,omitempty"` // if non-zero, forces a full re-index if the last index is older than this duration
}

type VolumeCreate struct {
	URL string `json:"url,omitempty" arg:"" help:"URL of the volume to create"`
	VolumeMeta
}

type Volume struct {
	VolumeCreate
	Name      string     `json:"name,omitempty"`
	CreatedAt time.Time  `json:"created_at,omitempty"`
	IndexedAt *time.Time `json:"indexed_at,omitempty"`
	Objects   uint64     `json:"objects,omitempty"`
}

type VolumeListRequest struct {
	Enabled *bool `json:"enabled,omitempty" help:"returns only enabled or disabled volumes" negatable:""`
	Stale   bool  `json:"stale,omitzero" help:"returns volumes that need to be re-indexed" negatable:""`
	pg.OffsetLimit
}

type VolumeList struct {
	VolumeListRequest
	Count uint64    `json:"count,omitempty"`
	Body  []*Volume `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (v Volume) String() string {
	return types.Stringify(v)
}

func (v VolumeMeta) String() string {
	return types.Stringify(v)
}

func (v VolumeListRequest) String() string {
	return types.Stringify(v)
}

func (v VolumeList) String() string {
	return types.Stringify(v)
}

////////////////////////////////////////////////////////////////////////////////
// QUERY

func (r VolumeListRequest) Query() url.Values {
	q := url.Values{}
	if r.Enabled != nil {
		q.Set("enabled", types.Stringify(*r.Enabled))
	}
	if r.Stale {
		q.Set("stale", "true")
	}
	if r.Offset != 0 {
		q.Set("offset", types.Stringify(r.Offset))
	}
	if r.Limit != nil {
		q.Set("limit", types.Stringify(*r.Limit))
	}
	return q
}

////////////////////////////////////////////////////////////////////////////////
// TABLE OUTPUT

func (r Volume) Header() []string {
	return []string{"Volume", "URL", "Enabled", "Created At", "Indexed Objects", "Index Delta", "Indexed At"}
}

func (r Volume) Width(col int) int {
	return 0
}

func (r Volume) Cell(col int) string {
	switch col {
	case 0:
		return r.Name
	case 1:
		return r.URL
	case 2:
		if r.Enabled == nil {
			return ""
		}
		return fmt.Sprint(*r.Enabled)
	case 3:
		return r.CreatedAt.Format(time.RFC3339)
	case 4:
		if r.IndexedAt == nil {
			return ""
		}
		return fmt.Sprint(r.Objects)
	case 5:
		if r.IndexDelta == nil {
			return "disabled"
		}
		return r.IndexDelta.String()
	case 6:
		if r.IndexedAt == nil {
			return ""
		}
		return r.IndexedAt.Format(time.RFC3339)
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (v *Volume) Scan(row pg.Row) error {
	return row.Scan(
		&v.Name,
		&v.URL,
		&v.Enabled,
		&v.IndexDelta,
		&v.CreatedAt,
		&v.IndexedAt,
		&v.Objects,
	)
}

func (v *VolumeList) Scan(row pg.Row) error {
	var volume Volume
	if err := volume.Scan(row); err != nil {
		return err
	} else {
		v.Body = append(v.Body, &volume)
	}
	return nil
}

func (v *VolumeList) ScanCount(row pg.Row) error {
	return row.Scan(&v.Count)
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (v VolumeName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	name := strings.ToLower(strings.TrimSpace(string(v)))
	if !types.IsIdentifier(name) {
		return "", gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	}
	bind.Set("name", name)

	switch op {
	case pg.Get:
		return bind.Query("filer.volume_get"), nil
	case pg.Update:
		return bind.Query("filer.volume_patch"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported VolumeName operation %q", op)
	}
}

func (v VolumeTouch) Select(bind *pg.Bind, op pg.Op) (string, error) {
	name := strings.ToLower(strings.TrimSpace(string(v)))
	if !types.IsIdentifier(name) {
		return "", gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	}
	bind.Set("name", name)

	switch op {
	case pg.Update:
		return bind.Query("filer.volume_touch"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported VolumeTouch operation %q", op)
	}
}

func (v *VolumeListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Set("orderby", "ORDER BY created_at DESC")

	// Where
	bind.Del("where")
	if v.Enabled != nil {
		bind.Append("where", `"enabled" = `+bind.Set("enabled", v.Enabled))
	}
	if v.Stale {
		bind.Append("where", `"enabled" = TRUE`)
		bind.Append("where", `"index_delta" IS NOT NULL`)
		bind.Append("where", `("indexed_at" IS NULL OR "indexed_at" < (NOW() - "index_delta"))`)
	}
	if where := bind.Join("where", " AND "); where != "" {
		bind.Set("where", `WHERE `+where)
	} else {
		bind.Set("where", "")
	}

	// Bind offset and limit
	v.OffsetLimit.Bind(bind, VolumeListLimit)

	// Return query
	switch op {
	case pg.List:
		return bind.Query("filer.volume_list"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported VolumeListRequest operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (v VolumeCreate) Insert(bind *pg.Bind) (string, error) {
	if name, ok := bind.Get("name").(string); !ok || !types.IsIdentifier(name) {
		return "", gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	}
	if rawURL := strings.TrimSpace(v.URL); rawURL == "" {
		return "", gofiler.ErrBadParameter.With("missing volume url")
	} else if _, err := url.Parse(rawURL); err != nil {
		return "", gofiler.ErrBadParameter.Withf("invalid volume url %q", rawURL)
	} else {
		bind.Set("url", rawURL)
	}

	if v.Enabled == nil {
		bind.Set("enabled", true)
	} else {
		bind.Set("enabled", v.Enabled)
	}

	bind.Set("index_delta", v.IndexDelta)

	return bind.Query("filer.volume_insert"), nil
}

func (v VolumeMeta) Insert(bind *pg.Bind) (string, error) {
	return "", gofiler.ErrBadParameter.With("volume meta insert is not supported; use VolumeCreate")
}

func (v VolumeMeta) Update(bind *pg.Bind) error {
	bind.Del("patch")

	if v.Enabled != nil {
		bind.Append("patch", `"enabled" = `+bind.Set("enabled", v.Enabled))
	}

	if v.IndexDelta != nil {
		if delta := types.Value(v.IndexDelta); delta >= 0 {
			bind.Append("patch", `"index_delta" = `+bind.Set("index_delta", v.IndexDelta))
		} else {
			return gofiler.ErrBadParameter.With("index_delta must be non-negative")
		}
	}

	if patch := bind.Join("patch", ", "); patch == "" {
		return gofiler.ErrBadParameter.With("no patch values")
	} else {
		bind.Set("patch", patch)
	}

	return nil
}
