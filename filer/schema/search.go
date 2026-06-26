package schema

import (
	"encoding/json"
	"fmt"
	"net/url"
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

type SearchListRequest struct {
	Volumes []string `json:"volumes" help:"List of volumes to search"`
	Text    string   `json:"query" arg:"" name:"query" help:"Search query"`
	pg.OffsetLimit
}

type SearchList struct {
	SearchListRequest
	Count uint64          `json:"count"`
	Body  []*SearchResult `json:"body"`
}

type SearchResult struct {
	Title   string  `json:"title"`
	Summary string  `json:"summary"`
	Rank    float64 `json:"rank"`
	Object
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (r SearchListRequest) String() string {
	return types.Stringify(r)
}

func (r SearchList) String() string {
	return types.Stringify(r)
}

func (r SearchResult) String() string {
	return types.Stringify(r)
}

////////////////////////////////////////////////////////////////////////////////
// QUERY

func (r SearchListRequest) Query() url.Values {
	query := url.Values{}
	if len(r.Volumes) > 0 {
		query.Set("volumes", strings.Join(r.Volumes, ","))
	}
	if r.Text != "" {
		query.Set("query", r.Text)
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
// TABLE

func (r SearchResult) Header() []string {
	return []string{"Volume", "Title", "Summary", "Rank", "Content Type", "Modified", "Meta"}
}

func (r SearchResult) Width(col int) int {
	return 0
}

func (r SearchResult) Cell(col int) string {
	switch col {
	case 0:
		return r.Volume
	case 1:
		if r.Title == "" {
			return r.Path
		}
		return r.Title + "\n" + r.Path
	case 2:
		summary := r.Summary + "\n"
		for _, kv := range r.Meta {
			if kv.Key != "tags" {
				continue
			}
			var tags []string
			if err := json.Unmarshal(kv.Value, &tags); err != nil {
				return summary
			}
			if summary != "" {
				summary += "\n"
			}
			summary += strings.Join(tags, ", ") + "\n"
			break
		}
		return summary
	case 3:
		return fmt.Sprintf("%.1f%%", r.Rank*100.0)
	case 4:
		return r.ContentType
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
			if kv.Key == "title" || kv.Key == "summary" || kv.Key == "tags" || kv.Key == "lyrics" || kv.Key == "lyrics-eng" {
				continue
			}
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
// SELECTOR

func (r *SearchListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Del("where")

	// Search query (required)
	query := strings.TrimSpace(r.Text)
	if query == "" {
		return "", httpresponse.ErrBadRequest.With("missing search query")
	} else {
		bind.Append("where", `s."tsv" @@ websearch_to_tsquery('english', `+bind.Set("query", query)+`)`)
	}

	// Volumes filter
	if len(r.Volumes) > 0 {
		bind.Append("where", `o."volume" = ANY(`+bind.Set("volumes", r.Volumes)+`)`)
	}

	// Construct the where clause
	if where := bind.Join("where", " AND "); where != "" {
		bind.Set("where", "WHERE "+where)
	} else {
		bind.Set("where", "")
	}

	// Bind offset and limit
	r.OffsetLimit.Bind(bind, SearchListLimit)

	switch op {
	case pg.List:
		return bind.Query("filer.search_list"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported SearchListRequest operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (l *SearchList) Scan(row pg.Row) error {
	var result SearchResult
	if err := result.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, &result)
	return nil
}

func (l *SearchList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

func (s *SearchResult) Scan(row pg.Row) error {
	var meta, artwork []byte

	// Step 1: scan object columns and rank
	if err := row.Scan(
		&s.Volume,
		&s.Path,
		&s.Size,
		&s.ContentType,
		&s.ETag,
		&s.ModTime,
		&meta,
		&artwork,
		&s.Rank,
	); err != nil {
		return err
	}

	if len(meta) > 0 {
		if err := json.Unmarshal(meta, &s.Meta); err != nil {
			return err
		}
	}

	if len(artwork) > 0 {
		if err := json.Unmarshal(artwork, &s.Artwork); err != nil {
			return err
		}
	}

	// Step 2: infer title and summary from meta kv pairs
	for _, kv := range s.Meta {
		switch strings.ToLower(kv.Key) {
		case "title":
			_ = json.Unmarshal(kv.Value, &s.Title)
		case "summary":
			_ = json.Unmarshal(kv.Value, &s.Summary)
		case "description":
			if s.Summary == "" {
				_ = json.Unmarshal(kv.Value, &s.Summary)
			}
		case "lyrics":
			if s.Summary == "" {
				_ = json.Unmarshal(kv.Value, &s.Summary)
			}
		case "lyrics-eng":
			if s.Summary == "" {
				_ = json.Unmarshal(kv.Value, &s.Summary)
			}
		}
	}

	return nil
}
