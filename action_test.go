package filer_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// createNote is an action, which creates a note of its text
type createNote struct{}

type createNoteInput struct {
	Text string `json:"text" help:"Text of the note"`
}

var _ gofiler.Action = createNote{}

func (createNote) Name() string        { return "create_note" }
func (createNote) Title() string       { return "Create note" }
func (createNote) Description() string { return "Create a note of its text" }
func (createNote) Input() *jsonschema.Schema {
	return jsonschema.MustFor[createNoteInput]()
}
func (createNote) Run(_ context.Context, input json.RawMessage) (*schema.Object, error) {
	var in createNoteInput
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, gofiler.ErrBadParameter.With(err)
	}
	return &schema.Object{ObjectKey: schema.ObjectKey{Path: "/" + in.Text + ".txt"}}, nil
}

func Test_ValidActionName(t *testing.T) {
	for _, name := range []string{"create_event", "update-task", "A1", strings.Repeat("a", 64)} {
		assert.True(t, gofiler.ValidActionName(name), name)
	}
	for _, name := range []string{"", "create event", "create.event", "créer", strings.Repeat("a", 65)} {
		assert.False(t, gofiler.ValidActionName(name), name)
	}
}

// An action is listed, without running it, and runs of its input
func Test_ActionOf(t *testing.T) {
	action := gofiler.ActionOf(createNote{})
	assert.Equal(t, "create_note", action.Name)
	assert.Equal(t, "Create note", action.Title)
	assert.Equal(t, "Create a note of its text", action.Description)
	require.NotNil(t, action.Input)

	// It's JSON, of its input's schema
	data, err := json.Marshal(action)
	require.NoError(t, err)
	var listed map[string]any
	require.NoError(t, json.Unmarshal(data, &listed))
	assert.Equal(t, "create_note", listed["name"])
	assert.Contains(t, string(data), `"text"`)

	object, err := createNote{}.Run(context.Background(), json.RawMessage(`{"text":"hello"}`))
	require.NoError(t, err)
	assert.Equal(t, "/hello.txt", object.Path)
}
