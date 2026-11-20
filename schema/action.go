package schema

import (
	"encoding/json"

	// Packages
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Action is something a volume's backend can do, beyond reading and writing
// its objects, such as creating a calendar event, as it's listed, without
// running it, whose input's schema is JSON, as a schema of a schema can't be
// described
type Action struct {
	Volume      string          `json:"volume,omitempty" help:"Name of the volume whose action it is"`
	Name        string          `json:"name" help:"Identifier of the action, such as create_event"`
	Title       string          `json:"title,omitempty" help:"Short label of the action, such as Create event"`
	Description string          `json:"description,omitempty" help:"What the action does"`
	Input       json.RawMessage `json:"input,omitempty" help:"JSON schema of the action's input"`
}

// ActionList is the actions of volumes, in order of their volumes' names,
// and their own
type ActionList struct {
	Body []Action `json:"body" help:"Actions, in order of their volumes, and their names"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (a Action) String() string {
	return types.Stringify(a)
}

func (a ActionList) String() string {
	return types.Stringify(a)
}
