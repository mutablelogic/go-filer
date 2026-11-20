package schema

import (
	"encoding/json"

	// Packages
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Action is something a volume's backend can do, beyond reading and writing
// its objects, such as creating a calendar event, as it's listed, without
// running it, whose input's schema is JSON, as a schema of a schema can't be
// described
type Action struct {
	Name        string          `json:"name" help:"Identifier of the action, such as create_event"`
	Title       string          `json:"title,omitempty" help:"Short label of the action, such as Create event"`
	Description string          `json:"description,omitempty" help:"What the action does"`
	Input       json.RawMessage `json:"input,omitempty" help:"JSON schema of the action's input"`
}

// ActionList is the actions of a volume, in order of their names
type ActionList struct {
	Name string   `json:"name" help:"Volume name"`
	Body []Action `json:"body" help:"Actions of the volume, in order of their names"`
}

////////////////////////////////////////////////////////////////////////////////
// TABLE

func (a Action) Header() []string {
	return []string{"Name", "Title", "Description"}
}

func (a Action) Width(col int) int {
	return 0
}

func (a Action) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return a.Name
	case 1:
		return a.Title
	case 2:
		return a.Description
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (a Action) String() string {
	return types.Stringify(a)
}

func (a ActionList) String() string {
	return types.Stringify(a)
}
