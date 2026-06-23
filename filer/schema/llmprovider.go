package schema

import (
	"net/url"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type LLMProviderName string

type LLMProviderMeta struct {
	Name       *string `json:"name,omitempty" help:"Provider name"`
	URL        *string `json:"url,omitempty" help:"Provider URL"`
	Credential *string `json:"credential,omitempty" help:"Credential key for authentication"`
}

type LLMProviderCreate struct {
	LLMProviderMeta
	Provider string `json:"provider,omitempty" arg:"" help:"Provider type (e.g. ollama, anthropic, openai)"`
}

type LLMProvider struct {
	LLMProviderCreate
	CreatedAt time.Time `json:"created_at,omitempty"`
}

type LLMProviderListRequest struct {
	pg.OffsetLimit
}

type LLMProviderList struct {
	LLMProviderListRequest
	Count uint64         `json:"count,omitempty"`
	Body  []*LLMProvider `json:"body,omitempty"`
}

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (l LLMProvider) String() string {
	return types.Stringify(l)
}

func (l LLMProviderMeta) String() string {
	return types.Stringify(l)
}

func (l LLMProviderCreate) String() string {
	return types.Stringify(l)
}

func (l LLMProviderListRequest) String() string {
	return types.Stringify(l)
}

func (l LLMProviderList) String() string {
	return types.Stringify(l)
}

///////////////////////////////////////////////////////////////////////////////
// QUERY

func (r LLMProviderListRequest) Query() url.Values {
	q := url.Values{}
	if r.Offset > 0 {
		q.Set("offset", types.Stringify(r.Offset))
	}
	if r.Limit != nil {
		q.Set("limit", types.Stringify(types.Value(r.Limit)))
	}
	return q
}

///////////////////////////////////////////////////////////////////////////////
// TABLE OUTPUT

func (r LLMProvider) Header() []string {
	return []string{"Name", "Provider", "URL", "Credential", "Created At"}
}

func (r LLMProvider) Width(col int) int {
	return 0
}

func (r LLMProvider) Cell(col int) string {
	switch col {
	case 0:
		return types.Value(r.Name)
	case 1:
		return r.Provider
	case 2:
		return types.Value(r.URL)
	case 3:
		if r.Credential == nil {
			return ""
		}
		return *r.Credential
	case 4:
		return r.CreatedAt.Format(time.RFC3339)
	default:
		return ""
	}
}

///////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (l LLMProviderName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	name := strings.TrimSpace(string(l))
	if !types.IsIdentifier(name) {
		return "", gofiler.ErrBadParameter.Withf("invalid provider name: %q", name)
	}
	bind.Set("name", name)

	switch op {
	case pg.Get:
		return bind.Query("llmprovider.get"), nil
	case pg.Update:
		return bind.Query("llmprovider.patch"), nil
	case pg.Delete:
		return bind.Query("llmprovider.delete"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported LLMProviderName operation %q", op)
	}
}

func (l *LLMProviderListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	l.OffsetLimit.Bind(bind, LLMProviderListLimit)

	switch op {
	case pg.List:
		return bind.Query("llmprovider.list"), nil
	default:
		return "", gofiler.ErrInternalServerError.Withf("unsupported LLMProviderListRequest operation %q", op)
	}
}

///////////////////////////////////////////////////////////////////////////////
// READER

func (l *LLMProvider) Scan(row pg.Row) error {
	if err := row.Scan(&l.Name, &l.Provider, &l.URL, &l.Credential, &l.CreatedAt); err != nil {
		return err
	}
	return nil
}

func (l *LLMProviderList) Scan(row pg.Row) error {
	var provider LLMProvider
	if err := provider.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, &provider)
	return nil
}

func (l *LLMProviderList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

///////////////////////////////////////////////////////////////////////////////
// WRITER

func (l LLMProviderCreate) Insert(bind *pg.Bind) (string, error) {
	name := strings.TrimSpace(types.Value(l.Name))
	provider := strings.TrimSpace(l.Provider)

	// Name is optional; if not provided, use provider as name
	if name == "" {
		name = provider
	}

	// Check name and provider
	if !types.IsIdentifier(name) {
		return "", gofiler.ErrBadParameter.Withf("invalid provider name: %q", name)
	} else {
		bind.Set("name", name)
	}
	if !types.IsIdentifier(provider) {
		return "", gofiler.ErrBadParameter.With("provider is required")
	} else {
		bind.Set("provider", provider)
	}

	// URL scheme should be http or https if provided
	if l.URL != nil {
		if url, err := url.Parse(types.Value(l.URL)); err != nil {
			return "", gofiler.ErrBadParameter.Withf("invalid url: %q", types.Value(l.URL))
		} else if url.Scheme != "http" && url.Scheme != "https" {
			return "", gofiler.ErrBadParameter.Withf("unsupported url scheme: %q", url.Scheme)
		} else {
			bind.Set("url", types.Value(l.URL))
		}
	}

	// Credential can be empty or should be a valid credential already stored in the system
	bind.Set("credential", l.Credential)

	return bind.Query("llmprovider.insert"), nil
}

func (l LLMProviderMeta) Insert(bind *pg.Bind) (string, error) {
	return "", gofiler.ErrBadParameter.With("llmprovider meta insert is not supported; use LLMProviderCreate")
}

func (l LLMProviderMeta) Update(bind *pg.Bind) error {
	bind.Del("patch")

	if name := strings.TrimSpace(types.Value(l.Name)); name != "" {
		if !types.IsIdentifier(name) {
			return gofiler.ErrBadParameter.Withf("invalid provider name: %q", name)
		}
		bind.Append("patch", `"name" = `+bind.Set("new_name", name))
	}

	// URL scheme should be http or https if provided
	if l.URL != nil {
		if url, err := url.Parse(types.Value(l.URL)); err != nil {
			return gofiler.ErrBadParameter.Withf("invalid url: %q", types.Value(l.URL))
		} else if url.Scheme != "http" && url.Scheme != "https" {
			return gofiler.ErrBadParameter.Withf("unsupported url scheme: %q", url.Scheme)
		} else {
			bind.Append("patch", `"url" = `+bind.Set("url", url.String()))
		}
	}

	if l.Credential != nil {
		bind.Append("patch", `"credential" = `+bind.Set("credential", l.Credential))
	}

	if patch := bind.Join("patch", ", "); patch == "" {
		return gofiler.ErrBadParameter.With("no patch values")
	} else {
		bind.Set("patch", patch)
	}

	return nil
}
