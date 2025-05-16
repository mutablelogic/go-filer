package schema

import (
	"encoding/json"

	// Packages
	pg "github.com/djthorpe/go-pg"
	llm "github.com/mutablelogic/go-llm"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Model struct {
	Name        string   `json:"name,omitempty" arg:"" name:"name" help:"Model name"`
	Description string   `json:"description,omitempty" name:"description" help:"Model description"`
	Aliases     []string `json:"aliases,omitempty" name:"aliases" help:"Model aliases"`
}

type ModelListRequest struct {
	pg.OffsetLimit
}

type ModelList struct {
	ModelListRequest
	Count uint64   `json:"count"`
	Body  []*Model `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func NewModel(model llm.Model) *Model {
	return &Model{
		Name:        model.Name(),
		Description: model.Description(),
		Aliases:     model.Aliases(),
	}
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (m Model) String() string {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (m ModelListRequest) String() string {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (m ModelList) String() string {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}
