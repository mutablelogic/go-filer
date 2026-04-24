package schema

import (
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type MetadataMeta struct {
	Title   *string  `json:"title,omitempty"`
	Summary *string  `json:"summary,omitempty"`
	Text    *string  `json:"text,omitempty"`
	Tags    []string `json:"tags,omitempty"`
}

type Metadata struct {
	ObjectKey
	MetadataMeta
	CreatedAt time.Time `json:"created_at,omitzero"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (m MetadataMeta) String() string {
	return types.Stringify(m)
}

func (m Metadata) String() string {
	return types.Stringify(m)
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (m *Metadata) Scan(row pg.Row) error {
	return row.Scan(
		&m.Name,
		&m.Path,
		&m.Title,
		&m.Summary,
		&m.Text,
		&m.Tags,
		&m.CreatedAt,
	)
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (m MetadataMeta) Insert(bind *pg.Bind) (string, error) {
	if !bind.Has("name") {
		return "", gofiler.ErrBadParameter.With("missing object name")
	}
	if !bind.Has("path") {
		return "", gofiler.ErrBadParameter.With("missing object path")
	}

	bind.Set("title", m.Title)
	bind.Set("summary", m.Summary)
	bind.Set("text", m.Text)

	tags := m.Tags
	if tags == nil {
		tags = []string{}
	}
	bind.Set("tags", tags)

	return bind.Query("filer.metadata_insert"), nil
}

func (m Metadata) Insert(bind *pg.Bind) (string, error) {
	bind.Set("name", m.Name)
	bind.Set("path", m.Path)
	return m.MetadataMeta.Insert(bind)
}

func (m MetadataMeta) Update(bind *pg.Bind) error {
	bind.Del("patch")

	if m.Title != nil {
		bind.Append("patch", `"title" = `+bind.Set("title", m.Title))
	}
	if m.Summary != nil {
		bind.Append("patch", `"summary" = `+bind.Set("summary", m.Summary))
	}
	if m.Text != nil {
		bind.Append("patch", `"text" = `+bind.Set("text", m.Text))
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
