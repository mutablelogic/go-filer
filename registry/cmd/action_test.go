package cmd

import (
	"strings"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// An action's input is its argument, or the standard input, of -, which is
// JSON, or {} when it's empty
func Test_ActionInput(t *testing.T) {
	input := func(cmd ActionCallCmd, stdin string) (string, error) {
		data, err := cmd.input(strings.NewReader(stdin))
		return string(data), err
	}
	got, err := input(ActionCallCmd{Action: "task_list", Input: `{"op":"create","title":"Work"}`}, "")
	require.NoError(t, err)
	assert.Equal(t, `{"op":"create","title":"Work"}`, got)
	got, err = input(ActionCallCmd{Action: "task_list"}, "")
	require.NoError(t, err)
	assert.Equal(t, "{}", got)
	got, err = input(ActionCallCmd{Action: "task_list", Input: "-"}, `{"op":"delete","path":"/L1"}`)
	require.NoError(t, err)
	assert.Equal(t, `{"op":"delete","path":"/L1"}`, got)

	// Input which isn't JSON, and an action whose name isn't valid
	_, err = input(ActionCallCmd{Action: "task_list", Input: "not json"}, "")
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = input(ActionCallCmd{Action: "task list", Input: "{}"}, "")
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
}
