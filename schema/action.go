package schema

import (
	// Packages
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Action is something a volume's backend can do, beyond reading and writing
// its objects, such as creating a calendar event, as it's listed, without
// running it
type Action struct {
	Name        string             `json:"name" help:"Identifier of the action, such as create_event"`
	Title       string             `json:"title,omitempty" help:"Short label of the action, such as Create event"`
	Description string             `json:"description,omitempty" help:"What the action does"`
	Input       *jsonschema.Schema `json:"input,omitempty" help:"JSON schema of the action's input"`
}

// ActionList is the actions of a volume, in order of their names
type ActionList struct {
	Name string   `json:"name" help:"Volume name"`
	Body []Action `json:"body" help:"Actions of the volume, in order of their names"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (a Action) String() string {
	return types.Stringify(a)
}

func (a ActionList) String() string {
	return types.Stringify(a)
}
