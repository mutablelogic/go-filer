package filer

import (
	"context"
	"encoding/json"
	"regexp"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Action is something a backend can do, beyond reading and writing its
// objects, such as creating a calendar event, which a Filer returns of
// Actions. It maps onto an LLM tool: its name, description and input are a
// tool's, and the object it returns is the tool's result.
type Action interface {
	// Name is the action's identifier, in snake_case, such as create_event,
	// which is a tool's name as it is (see ValidActionName)
	Name() string

	// Title is a short label of the action, such as "Create event"
	Title() string

	// Description is what the action does, for people and models
	Description() string

	// Input is the JSON schema of the action's input
	Input() *jsonschema.Schema

	// Run does the action, with its input, which is JSON of its schema, and
	// returns the object it created or changed
	Run(ctx context.Context, input json.RawMessage) (*schema.Object, error)
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// actionName is the name of an action, which is a tool's name
var actionName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// ValidActionName returns true when the name of an action is valid: of
// letters, digits, "_" and "-", of up to 64 characters, as a tool's name is
func ValidActionName(name string) bool {
	return actionName.MatchString(name)
}

// ActionOf returns what an action is, without running it, as it's listed,
// such as over HTTP
func ActionOf(action Action) schema.Action {
	result := schema.Action{
		Name:        action.Name(),
		Title:       action.Title(),
		Description: action.Description(),
	}
	if input := action.Input(); input != nil {
		if data, err := json.Marshal(input); err == nil {
			result.Input = data
		}
	}
	return result
}
